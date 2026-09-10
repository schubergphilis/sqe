// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! Index b-tree delete (write path): entry delete plus underflow
//! handling. Mirrors `delete.rs` (table delete) in spirit but not in
//! mechanism — `insert.rs`'s module doc explains why: index b-tree
//! interior cells carry a full entry (its own value), not just a routing
//! key, so a delete target may be found sitting at interior level rather
//! than in a leaf, and removing a routing entry can never be conflated
//! with discarding whatever value that entry itself carries.
//!
//! **Underflow policy.** An emptied leaf (or an interior page that drains
//! to zero of its own entries) is simply **left in place** — 0 cells is a
//! structurally valid page, still correctly reachable via its parent's
//! existing child/rightmost pointer, so no entry ever needs adjusting
//! just because a child became empty. This is a deliberate simplification
//! of SQLite's proactive sibling-redistributing `balance()` (see
//! `delete.rs`'s module doc for the same philosophy applied to tables):
//! it trades away freelist reuse of drained pages for a much simpler,
//! harder-to-get-wrong implementation. The one case that DOES need
//! structural surgery is covered by [`extract_max_entry`] below.
//!
//! **Interior-match deletion (predecessor swap).** When
//! [`super::descend_index_tree`] reports
//! [`IndexDescent::InteriorMatch`] (the target key IS an interior page's
//! own entry), the entry can't simply be dropped — its child pointer is
//! load-bearing (routes to the subtree of lesser keys) and, separately,
//! *other* interior entries along the way to a replacement each carry
//! their own live value that must never be discarded as a side effect of
//! removing routing. [`extract_max_entry`] recursively finds and
//! physically removes the maximum entry of `entry_child`'s subtree (its
//! in-order predecessor), correctly falling back to an interior page's
//! own last entry (rather than erroring) when that page's `rightmost`
//! subtree turns out to already be fully drained — the entry it pops in
//! that fallback is handled by promoting its own child to the new
//! `rightmost`, never dropping the live data that child still holds. If
//! `entry_child`'s entire subtree is drained (no predecessor at all —
//! everything in it was already deleted earlier), the matched entry is
//! removed outright instead of swapped.

use crate::btree::index::{
    build_index_interior_cell, collect_index_interior_entries, collect_index_leaf_cells,
    decode_payload_len, descend_index_tree, search_index_leaf, write_index_interior_page,
    IndexDescent, LeafSearch, INTERIOR_INDEX, LEAF_INDEX,
};
use crate::btree::{
    local_payload_size, page1_header_start, read_page_type, read_u32, splice_delete_cell,
    BtreeError, MAX_PAGES_VISITED,
};
use crate::header::DatabaseHeader;
use crate::pager::Pager;
use crate::record::{TextEncoding, Value};

/// Returns the first overflow page of a value cell's raw bytes (`0` if
/// its payload is entirely local). `value_bytes` is the verbatim
/// `payload-length varint + local bytes [+ 4-byte overflow pointer]`
/// shape [`collect_index_leaf_cells`]/[`collect_index_interior_entries`]
/// already extract — decoding it directly (rather than re-reading from a
/// live page) works because that shape is self-contained, with the
/// payload-length varint at offset 0.
fn overflow_page_of(value_bytes: &[u8], usable_size: u32) -> Result<u32, BtreeError> {
    let (payload_len, tail_start) = decode_payload_len(value_bytes, 0, 0)?;
    let local_size = local_payload_size(usable_size, payload_len, true) as usize;
    if (local_size as u64) < payload_len {
        Ok(read_u32(
            value_bytes,
            tail_start.saturating_add(local_size),
            0,
        )?)
    } else {
        Ok(0)
    }
}

/// Walks and frees an overflow-page chain starting at `first_page` (a
/// no-op if it's `0` — the cell had no overflow). Mirrors
/// `table::delete::free_overflow_chain` (duplicated rather than shared —
/// see `insert.rs`'s `write_overflow_chain` doc comment for why).
fn free_overflow_chain(pager: &mut Pager, first_page: u32) -> Result<(), BtreeError> {
    let mut page_num = first_page;
    let mut visited = std::collections::HashSet::new();
    while page_num != 0 {
        if !visited.insert(page_num) {
            return Err(BtreeError::OverflowChainCycle {
                page_num: first_page,
                revisited_page: page_num,
            });
        }
        let next = {
            let buf = pager.get_page_mut(page_num)?;
            read_u32(buf, 0, page_num)?
        };
        pager.deallocate_page(page_num)?;
        page_num = next;
    }
    Ok(())
}

/// Deletes the entry with exactly `key` (via `compare_keys`) from the
/// index b-tree rooted at `root_page`. Returns `Err(BtreeError::KeyNotFound)`
/// if no such entry exists, leaving the tree unchanged. See the module
/// doc for how a target found at interior level (not just in a leaf) is
/// handled.
pub fn delete_entry(
    pager: &mut Pager,
    header: &DatabaseHeader,
    root_page: u32,
    key: &[Value],
    encoding: TextEncoding,
) -> Result<(), BtreeError> {
    let usable_size = header.usable_page_size();
    match descend_index_tree(pager, root_page, usable_size, key, encoding)? {
        IndexDescent::Leaf { leaf_page, .. } => {
            delete_from_leaf(pager, usable_size, leaf_page, key, encoding)
        }
        IndexDescent::InteriorMatch {
            interior_page,
            entry_child,
        } => delete_via_predecessor_swap(pager, usable_size, interior_page, entry_child, encoding),
    }
}

/// Removes the cell matching `key` from `leaf_page` (an ordinary
/// leaf-level delete — `key` was found to genuinely live there). Writes
/// the leaf with whatever cells remain, even zero — see the module doc's
/// underflow policy.
fn delete_from_leaf(
    pager: &mut Pager,
    usable_size: u32,
    leaf_page: u32,
    key: &[Value],
    encoding: TextEncoding,
) -> Result<(), BtreeError> {
    let header_start = page1_header_start(leaf_page);
    let buf = pager.get_page_mut(leaf_page)?.clone();

    // Binary search for the exact match, decoding only the O(log n)
    // cells actually compared instead of every cell on the page (#648).
    let (pos, matched_cell) = match search_index_leaf(
        pager,
        &buf,
        header_start,
        leaf_page,
        usable_size,
        encoding,
        key,
    )? {
        LeafSearch::Found(pos, cell) => (pos, cell),
        LeafSearch::NotFound(_) => return Err(BtreeError::KeyNotFound),
    };
    let overflow_page = overflow_page_of(&matched_cell.1, usable_size)?;

    let buf = pager.get_page_mut(leaf_page)?;
    splice_delete_cell(buf, header_start, leaf_page, usable_size, pos, false)?;
    free_overflow_chain(pager, overflow_page)
}

/// Handles a delete target found at interior level — see the module doc
/// for the predecessor-swap algorithm.
fn delete_via_predecessor_swap(
    pager: &mut Pager,
    usable_size: u32,
    interior_page: u32,
    entry_child: u32,
    encoding: TextEncoding,
) -> Result<(), BtreeError> {
    match extract_max_entry(pager, usable_size, entry_child, encoding, 0)? {
        Some(predecessor_bytes) => {
            let header_start = page1_header_start(interior_page);
            let buf = pager.get_page_mut(interior_page)?.clone();
            let (mut entries, rightmost) = collect_index_interior_entries(
                pager,
                &buf,
                header_start,
                interior_page,
                usable_size,
                encoding,
            )?;
            let entry = entries
                .iter_mut()
                .find(|(child, _, _)| *child == entry_child)
                .ok_or(BtreeError::Internal(
                    "entry_child's routing entry must still exist in interior_page",
                ))?;
            entry.2 = predecessor_bytes;
            let cell_bytes: Vec<Vec<u8>> = entries
                .iter()
                .map(|(child, _, value_bytes)| build_index_interior_cell(*child, value_bytes))
                .collect();
            let buf = pager.get_page_mut(interior_page)?;
            write_index_interior_page(buf, header_start, interior_page, &cell_bytes, rightmost)
        }
        None => {
            // `entry_child`'s entire subtree is drained (nothing left to
            // swap in) — the matched entry is deleted outright, since
            // there is nothing left to preserve.
            pager.deallocate_page(entry_child)?;
            remove_entry_by_child(pager, usable_size, interior_page, entry_child, encoding)
        }
    }
}

/// Recursively finds and physically removes the maximum entry within the
/// subtree rooted at `page_num`, returning its raw cell bytes — or `None`
/// if that subtree holds no entries at all. See the module doc: this is
/// the one operation in this module that must actively restructure pages
/// (rather than just leaving emptied ones in place), because an
/// interior page's own last entry may need to become the new `rightmost`
/// once its previous `rightmost` subtree is confirmed drained.
///
/// Whenever this function determines that the subtree rooted at
/// `page_num` is itself fully drained (returning `Ok(None)`), it has
/// already deallocated every descendant page of `page_num` — never just
/// the immediate child — so a caller that sees `None` only needs to
/// deallocate `page_num` itself, not walk its subtree. `depth` guards
/// against a corrupted/cyclic `rightmost` chain causing unbounded
/// recursion, mirroring [`descend_index_tree`]'s `MAX_PAGES_VISITED`
/// convention.
fn extract_max_entry(
    pager: &mut Pager,
    usable_size: u32,
    page_num: u32,
    encoding: TextEncoding,
    depth: usize,
) -> Result<Option<Vec<u8>>, BtreeError> {
    if depth > MAX_PAGES_VISITED {
        return Err(BtreeError::TraversalTooLong {
            max: MAX_PAGES_VISITED,
        });
    }
    let header_start = page1_header_start(page_num);
    let buf = pager.get_page_mut(page_num)?.clone();
    let page_type = read_page_type(&buf, header_start, page_num)?;

    if page_type == LEAF_INDEX {
        let cells =
            collect_index_leaf_cells(pager, &buf, header_start, page_num, usable_size, encoding)?;
        let Some((_, max_bytes)) = cells.last().cloned() else {
            return Ok(None);
        };
        let last_idx = cells.len().saturating_sub(1);
        let buf = pager.get_page_mut(page_num)?;
        splice_delete_cell(buf, header_start, page_num, usable_size, last_idx, false)?;
        return Ok(Some(max_bytes));
    }
    if page_type != INTERIOR_INDEX {
        return Err(BtreeError::UnexpectedPageType {
            page_num,
            page_type,
        });
    }

    let (mut entries, rightmost) =
        collect_index_interior_entries(pager, &buf, header_start, page_num, usable_size, encoding)?;

    if let Some(max_bytes) = extract_max_entry(
        pager,
        usable_size,
        rightmost,
        encoding,
        depth.saturating_add(1),
    )? {
        // `rightmost`'s subtree had the maximum; it already rewrote
        // whichever page(s) it touched. This page's own shape (entries,
        // rightmost) is unaffected.
        return Ok(Some(max_bytes));
    }

    // `rightmost`'s subtree is fully drained — the recursive call above
    // has already deallocated every page in it. This page's own last
    // entry — if it has one — is the true maximum; its child becomes the
    // new `rightmost` so the (still possibly live) data under it stays
    // reachable.
    let Some((child, _, max_bytes)) = entries.pop() else {
        // This page's own entries are empty too, so `page_num` itself is
        // now fully drained. `rightmost` was already confirmed drained
        // above (and its own descendants already freed by the recursive
        // call) but never itself deallocated — do that now, since our
        // caller sees only `None` and has no other way to learn that
        // `rightmost` still needs freeing.
        pager.deallocate_page(rightmost)?;
        return Ok(None);
    };
    pager.deallocate_page(rightmost)?;
    let cell_bytes: Vec<Vec<u8>> = entries
        .iter()
        .map(|(c, _, value_bytes)| build_index_interior_cell(*c, value_bytes))
        .collect();
    let buf = pager.get_page_mut(page_num)?;
    write_index_interior_page(buf, header_start, page_num, &cell_bytes, child)?;
    Ok(Some(max_bytes))
}

/// Removes the entry whose child is `child_to_remove` from `page_num`,
/// leaving `rightmost` and every other entry untouched (even if this
/// leaves `page_num` with zero entries — see the module doc's underflow
/// policy).
fn remove_entry_by_child(
    pager: &mut Pager,
    usable_size: u32,
    page_num: u32,
    child_to_remove: u32,
    encoding: TextEncoding,
) -> Result<(), BtreeError> {
    let header_start = page1_header_start(page_num);
    let buf = pager.get_page_mut(page_num)?.clone();
    let (mut entries, rightmost) =
        collect_index_interior_entries(pager, &buf, header_start, page_num, usable_size, encoding)?;
    let idx = entries
        .iter()
        .position(|(child, _, _)| *child == child_to_remove)
        .ok_or(BtreeError::MissingChildRoute {
            page_num,
            child: child_to_remove,
        })?;
    let (_, _, removed_value_bytes) = entries.remove(idx);
    let overflow_page = overflow_page_of(&removed_value_bytes, usable_size)?;
    let cell_bytes: Vec<Vec<u8>> = entries
        .iter()
        .map(|(c, _, value_bytes)| build_index_interior_cell(*c, value_bytes))
        .collect();
    let buf = pager.get_page_mut(page_num)?;
    write_index_interior_page(buf, header_start, page_num, &cell_bytes, rightmost)?;
    free_overflow_chain(pager, overflow_page)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing, clippy::panic)]
mod tests {
    use super::*;
    use crate::btree::index::insert::insert_entry;
    use crate::btree::index::write_index_leaf_page;
    use crate::vfs::{MemoryVfs, PageSource};
    use std::path::Path;

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
    fn deleting_a_missing_key_errors() {
        let page_size = 512u32;
        let (vfs, header) = minimal_index_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();

        insert_entry(&mut pager, &header, 1, &key("a", 1), TextEncoding::Utf8).unwrap();
        let err =
            delete_entry(&mut pager, &header, 1, &key("b", 2), TextEncoding::Utf8).unwrap_err();
        assert!(matches!(err, BtreeError::KeyNotFound));
    }

    #[test]
    fn deleting_the_only_entry_leaves_an_empty_root_leaf() {
        let page_size = 512u32;
        let (vfs, header) = minimal_index_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();

        insert_entry(&mut pager, &header, 1, &key("a", 1), TextEncoding::Utf8).unwrap();
        delete_entry(&mut pager, &header, 1, &key("a", 1), TextEncoding::Utf8).unwrap();

        let header_start = page1_header_start(1);
        let buf = pager.get_page_mut(1).unwrap().clone();
        let page_type = read_page_type(&buf, header_start, 1).unwrap();
        assert_eq!(page_type, LEAF_INDEX);
        let cells = collect_index_leaf_cells(
            &pager,
            &buf,
            header_start,
            1,
            header.usable_page_size(),
            TextEncoding::Utf8,
        )
        .unwrap();
        assert!(cells.is_empty());
    }

    /// Regression test: index cells use a smaller `max_local` than table
    /// leaf cells (`(usable_size-12)*64/255-23`, not `usable_size-35` —
    /// 006-btree Requirement 7 flagged this via a real fixture), so a key
    /// well under the table threshold can still overflow here. Deleting
    /// such an entry must free its overflow chain, not leak it — mirrors
    /// `table::delete::tests::deleting_a_row_with_overflow_frees_its_overflow_chain`.
    #[test]
    fn deleting_an_entry_with_overflow_frees_its_overflow_chain() {
        let page_size = 512u32;
        let (vfs, header) = minimal_index_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();

        // `max_local` for a 512-byte page is (512-12)*64/255-23 = 102, so
        // 300 bytes of key text forces an overflow chain.
        let big_key = key(&"x".repeat(300), 1);
        insert_entry(&mut pager, &header, 1, &big_key, TextEncoding::Utf8).unwrap();

        let freelist_before = freelist_page_count(&mut pager);
        assert_eq!(freelist_before, 0);
        delete_entry(&mut pager, &header, 1, &big_key, TextEncoding::Utf8).unwrap();
        let freelist_after = freelist_page_count(&mut pager);

        assert!(
            freelist_after > freelist_before,
            "the entry's overflow chain must be returned to the freelist on delete"
        );
    }

    fn freelist_page_count(pager: &mut Pager) -> u32 {
        let page1 = pager.get_page_mut(1).unwrap().clone();
        u32::from_be_bytes(page1[36..40].try_into().unwrap())
    }

    #[test]
    fn deleting_one_of_two_entries_keeps_the_other() {
        let page_size = 512u32;
        let (vfs, header) = minimal_index_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();

        insert_entry(&mut pager, &header, 1, &key("a", 1), TextEncoding::Utf8).unwrap();
        insert_entry(&mut pager, &header, 1, &key("b", 2), TextEncoding::Utf8).unwrap();
        delete_entry(&mut pager, &header, 1, &key("a", 1), TextEncoding::Utf8).unwrap();

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
        assert_eq!(cells.len(), 1);
        assert_eq!(cells[0].0, key("b", 2));
    }

    #[test]
    fn minimal_two_entry_split_then_delete_promoted_key() {
        let page_size = 512u32;
        let (vfs, header) = minimal_index_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();

        let filler = "x".repeat(190);
        let k0 = vec![
            Value::Text(format!("{filler}-0001").into()),
            Value::Integer(1),
        ];
        let k1 = vec![
            Value::Text(format!("{filler}-0002").into()),
            Value::Integer(2),
        ];
        insert_entry(&mut pager, &header, 1, &k0, TextEncoding::Utf8).unwrap();
        insert_entry(&mut pager, &header, 1, &k1, TextEncoding::Utf8).unwrap();
        delete_entry(&mut pager, &header, 1, &k1, TextEncoding::Utf8).unwrap();
    }

    #[test]
    fn split_then_delete_all_including_promoted_interior_entries() {
        // Small pages + ~200-byte keys force splits (and therefore
        // entries promoted to interior level) well before 30 entries —
        // regression guard for the interior-match predecessor swap:
        // deleting every entry, in ascending order, must never error and
        // must leave the tree fully empty (per the read-side cursor).
        let page_size = 512u32;
        let (vfs, header) = minimal_index_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();

        let filler = "x".repeat(190);
        let n = 30i64;
        let keys: Vec<Vec<Value>> = (1..=n)
            .map(|i| {
                vec![
                    Value::Text(format!("{filler}-{i:04}").into()),
                    Value::Integer(i),
                ]
            })
            .collect();
        for k in &keys {
            insert_entry(&mut pager, &header, 1, k, TextEncoding::Utf8).unwrap();
        }
        for (idx, k) in keys.iter().enumerate() {
            let r = delete_entry(&mut pager, &header, 1, k, TextEncoding::Utf8);
            if let Err(e) = r {
                panic!("delete failed at idx {idx}: {:?}", e);
            }
        }

        let mut cursor = crate::btree::IndexCursor::new(pager, header.usable_page_size(), 1);
        assert!(cursor.first().unwrap().is_none());
    }

    /// Walks every page number reachable from `page_num` (an index b-tree
    /// root/subtree): itself, plus (for an interior page) every entry's
    /// child and `rightmost`, recursively.
    fn reachable_pages(pager: &Pager, page_num: u32, encoding: TextEncoding, out: &mut Vec<u32>) {
        out.push(page_num);
        let header_start = page1_header_start(page_num);
        let buf = pager.read_page(page_num).unwrap();
        let page_type = read_page_type(&buf, header_start, page_num).unwrap();
        if page_type != INTERIOR_INDEX {
            return;
        }
        let (entries, rightmost) = collect_index_interior_entries(
            pager,
            &buf,
            header_start,
            page_num,
            buf.len() as u32,
            encoding,
        )
        .unwrap();
        for (child, _, _) in &entries {
            reachable_pages(pager, *child, encoding, out);
        }
        reachable_pages(pager, rightmost, encoding, out);
    }

    /// Walks the freelist trunk chain (page 1's header fields), returning
    /// every page number currently on it (trunks and leaves alike).
    fn freelist_pages(pager: &Pager) -> Vec<u32> {
        let page1 = pager.read_page(1).unwrap();
        let mut trunk = u32::from_be_bytes([page1[32], page1[33], page1[34], page1[35]]);
        let mut out = Vec::new();
        while trunk != 0 {
            out.push(trunk);
            let buf = pager.read_page(trunk).unwrap();
            let parsed = crate::pager::freelist::TrunkPage::parse(&buf).unwrap();
            out.extend(&parsed.leaves);
            trunk = parsed.next_trunk;
        }
        out
    }

    #[test]
    fn deleting_all_entries_orphans_no_page() {
        // Regression guard for the `extract_max_entry` fix: a predecessor
        // swap that drains a subtree more than one level deep (interior ->
        // interior -> leaf) must deallocate every page in that subtree,
        // not just the immediate child — otherwise a deeper emptied page
        // becomes unreachable garbage (neither reachable from the root
        // nor on the freelist) instead of landing on the freelist. Every
        // page in the file must be either reachable from the root or on
        // the freelist — nothing left in neither set.
        let page_size = 512u32;
        let (vfs, header) = minimal_index_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();

        let filler = "x".repeat(190);
        let n = 30i64;
        let keys: Vec<Vec<Value>> = (1..=n)
            .map(|i| {
                vec![
                    Value::Text(format!("{filler}-{i:04}").into()),
                    Value::Integer(i),
                ]
            })
            .collect();
        for k in &keys {
            insert_entry(&mut pager, &header, 1, k, TextEncoding::Utf8).unwrap();
        }
        for k in &keys {
            delete_entry(&mut pager, &header, 1, k, TextEncoding::Utf8).unwrap();
        }

        let raw = pager.get_page_mut(1).unwrap().clone();
        let total_pages = u32::from_be_bytes([raw[28], raw[29], raw[30], raw[31]]);

        let mut reachable = Vec::new();
        reachable_pages(&pager, 1, TextEncoding::Utf8, &mut reachable);
        let freed = freelist_pages(&pager);

        let mut accounted: Vec<u32> = reachable.into_iter().chain(freed).collect();
        accounted.sort_unstable();
        accounted.dedup();
        let all_pages: Vec<u32> = (1..=total_pages).collect();
        assert_eq!(
            accounted, all_pages,
            "every page must be either reachable from the root or on the freelist — \
             a page missing from both is orphaned"
        );
    }

    /// Kills the `depth > MAX_PAGES_VISITED` -> `==`/`>=` mutants: at
    /// exactly `MAX_PAGES_VISITED`, the real `>` guard has NOT yet
    /// tripped (only depths strictly greater than the max are rejected),
    /// so a call at that exact depth must still succeed.
    #[test]
    fn extract_max_entry_does_not_reject_depth_exactly_at_the_limit() {
        let page_size = 512u32;
        let (vfs, header) = minimal_index_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();

        insert_entry(&mut pager, &header, 1, &key("a", 1), TextEncoding::Utf8).unwrap();

        let result = extract_max_entry(
            &mut pager,
            header.usable_page_size(),
            1,
            TextEncoding::Utf8,
            MAX_PAGES_VISITED,
        )
        .unwrap();
        assert!(
            result.is_some(),
            "depth == MAX_PAGES_VISITED must still be processed, not rejected"
        );
    }

    /// The mirror case: one past the limit must be rejected.
    #[test]
    fn extract_max_entry_rejects_depth_past_the_limit() {
        let page_size = 512u32;
        let (vfs, header) = minimal_index_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();

        insert_entry(&mut pager, &header, 1, &key("a", 1), TextEncoding::Utf8).unwrap();

        let err = extract_max_entry(
            &mut pager,
            header.usable_page_size(),
            1,
            TextEncoding::Utf8,
            MAX_PAGES_VISITED + 1,
        )
        .unwrap_err();
        assert!(matches!(err, BtreeError::TraversalTooLong { .. }));
    }
}
