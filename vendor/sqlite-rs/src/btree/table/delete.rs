// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! Table b-tree delete (write path): cell delete plus page
//! merge/collapse on underflow. See `.openspec/specs/006-btree/spec.md`
//! (delete/collapse requirements) for the byte-layout contract this
//! module writes. Read-side and shared write-path helpers
//! (`page1_header_start`, `read_page_type`, `find_leaf_page`,
//! `collect_leaf_cells`, `collect_interior_entries`, `build_interior_cell`,
//! `write_leaf_page`, `write_interior_page`) are reused directly from the
//! parent `btree` module. A single-cell delete that doesn't empty (or
//! collapse) the page goes through `splice_delete_cell` instead (O(1)
//! relative to the page's other cells) — see `insert.rs`'s module doc and
//! `.openspec/adr/0023-leaf-cell-splice.md` (#337).
//!
//! Underflow policy (this ticket's "page merge/rebalance" scope item):
//! rather than porting SQLite's exact 3-sibling balance algorithm (which
//! proactively redistributes/merges pages once they drop below a
//! half-full threshold), this module only collapses a page when a delete
//! leaves it **completely empty**. An empty leaf is removed from its
//! parent and deallocated (returned to the freelist, #167); if that
//! leaves the parent itself with zero routing entries (just its
//! `rightmost` pointer), the parent is collapsed the same way, cascading
//! up to the root if necessary. This keeps every page a delete leaves
//! behind non-empty and structurally valid — sufficient for
//! `PRAGMA integrity_check` and for freed pages to be reused by a later
//! insert via the freelist — without implementing proactive half-full
//! redistribution, which nothing else in this codebase's rebuild-from-
//! scratch write path does either.

use crate::btree::{
    build_interior_cell, cell_bytes, collect_interior_entries, collect_leaf_cells, find_leaf_cell,
    find_leaf_page, page1_header_start, read_page_type, read_u32, splice_delete_cell,
    write_interior_page, write_leaf_page, BtreeError, INTERIOR_TABLE, LEAF_TABLE,
};
use crate::header::DatabaseHeader;
use crate::pager::Pager;

/// Deletes the row with `rowid` from the table b-tree rooted at
/// `root_page`, collapsing an emptied leaf (and cascading into ancestors,
/// up to `root_page` itself) as needed. Returns
/// `Err(BtreeError::RowidNotFound)` if no such row exists, leaving the
/// tree unchanged.
pub fn delete_row(
    pager: &mut Pager,
    header: &DatabaseHeader,
    root_page: u32,
    rowid: i64,
) -> Result<(), BtreeError> {
    let usable_size = header.usable_page_size();
    let (ancestors, leaf_page) = find_leaf_page(pager, root_page, rowid)?;

    let header_start = page1_header_start(leaf_page);
    // Zero-copy pre-scan (#588): the splice fast path only needs the
    // cell's position and its overflow pointer, so the page is never
    // cloned and no cell is materialized.
    let buf = pager.get_page_mut(leaf_page)?;
    let (pos, num_cells, overflow_page) =
        find_leaf_cell(buf, header_start, leaf_page, usable_size, rowid)?
            .ok_or(BtreeError::RowidNotFound { rowid })?;

    if num_cells > 1 || ancestors.is_empty() {
        // Either the leaf still holds rows after this delete, or it's the
        // root itself (which can't be removed/collapsed — an empty root
        // leaf is a valid, empty table). Splice the one cell out in place
        // (O(1) relative to the page's other cells) rather than
        // collecting and rewriting every surviving cell.
        splice_delete_cell(buf, header_start, leaf_page, usable_size, pos, true)?;
        return free_overflow_chain(pager, overflow_page);
    }

    // The page held exactly this one cell and has ancestors: it empties.
    write_leaf_page(buf, header_start, leaf_page, &[])?;
    pager.deallocate_page(leaf_page)?;
    free_overflow_chain(pager, overflow_page)?;
    collapse_into_ancestors(pager, usable_size, root_page, &ancestors, leaf_page)
}

/// Walks an overflow chain starting at `first_page` (a no-op if it's `0`,
/// i.e. the deleted cell had no overflow), deallocating every page in the
/// chain back to the freelist (#167). Guards against a corrupt/cyclic
/// chain the same way [`super::reassemble_payload`] does, so a malformed
/// on-disk chain fails deletion cleanly rather than looping forever.
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
        // Only the 4-byte next-pointer is needed — never clone the page.
        let next = {
            let buf = pager.get_page_mut(page_num)?;
            read_u32(buf, 0, page_num)?
        };
        pager.deallocate_page(page_num)?;
        page_num = next;
    }
    Ok(())
}

/// Removes the routing entry for `emptied_page` from its immediate parent
/// (the last entry in `ancestors`), then — if that leaves the parent with
/// no routing entries at all (down to just its `rightmost` pointer) —
/// recursively collapses the parent into its own parent (or, if the
/// parent is the root, relocates the sole remaining child's content into
/// the root page in place, mirroring `insert.rs::root_split` in reverse).
fn collapse_into_ancestors(
    pager: &mut Pager,
    usable_size: u32,
    root_page: u32,
    ancestors: &[u32],
    emptied_page: u32,
) -> Result<(), BtreeError> {
    let Some((&parent_page, rest)) = ancestors.split_last() else {
        return Err(BtreeError::Internal(
            "collapse_into_ancestors called with no ancestors",
        ));
    };

    let header_start = page1_header_start(parent_page);
    let (mut entries, mut rightmost) = {
        let buf = pager.get_page_mut(parent_page)?;
        collect_interior_entries(buf, header_start, parent_page)?
    };

    match entries.iter().position(|(child, _)| *child == emptied_page) {
        Some(idx) => {
            entries.remove(idx);
        }
        None if rightmost == emptied_page => {
            let Some((last_child, _)) = entries.pop() else {
                return Err(BtreeError::Internal(
                    "interior page's rightmost pointer was emptied but it has no routing entries to promote",
                ));
            };
            rightmost = last_child;
        }
        None => {
            return Err(BtreeError::MissingChildRoute {
                page_num: parent_page,
                child: emptied_page,
            });
        }
    }

    if !entries.is_empty() {
        let cell_bytes: Vec<Vec<u8>> = entries
            .iter()
            .map(|(child, key)| build_interior_cell(*child, *key))
            .collect();
        let buf = pager.get_page_mut(parent_page)?;
        return write_interior_page(buf, header_start, parent_page, &cell_bytes, rightmost);
    }

    // The parent now has zero routing entries — its only remaining child
    // is `rightmost`, which may still hold real, live rows (it's
    // unrelated to whichever entry/child just emptied). It no longer
    // earns its own page — but recursing into `collapse_into_ancestors`
    // again here (treating `parent_page` itself as "emptied") would
    // silently drop `rightmost`'s entire subtree: the grandparent's
    // handling of "child `parent_page` emptied" removes/repoints its
    // reference to `parent_page`, with nothing to carry `rightmost`
    // forward. The correct operation is a splice: replace whichever
    // reference pointed at `parent_page` in ITS OWN parent with
    // `rightmost` directly, leaving the grandparent's entry count (and
    // every key) otherwise unchanged — never a further collapse cascade.
    if parent_page == root_page {
        return collapse_root(pager, usable_size, root_page, rightmost);
    }

    pager.deallocate_page(parent_page)?;
    splice_child(pager, rest, parent_page, rightmost)
}

/// Replaces every reference to `old_child` in its immediate parent (the
/// last entry in `ancestors`) with `new_child`, leaving that parent's
/// entry count (and every key) otherwise unchanged. See
/// `collapse_into_ancestors`'s doc for why this — not a further collapse
/// cascade — is the correct response to an interior page draining to a
/// single surviving child.
fn splice_child(
    pager: &mut Pager,
    ancestors: &[u32],
    old_child: u32,
    new_child: u32,
) -> Result<(), BtreeError> {
    let Some((&parent_page, _)) = ancestors.split_last() else {
        return Err(BtreeError::Internal(
            "splice_child called with no ancestors — old_child's parent must always exist here (it's never the root)",
        ));
    };

    let header_start = page1_header_start(parent_page);
    let (mut entries, mut rightmost) = {
        let buf = pager.get_page_mut(parent_page)?;
        collect_interior_entries(buf, header_start, parent_page)?
    };

    match entries.iter_mut().find(|(child, _)| *child == old_child) {
        Some(entry) => entry.0 = new_child,
        None if rightmost == old_child => rightmost = new_child,
        None => {
            return Err(BtreeError::MissingChildRoute {
                page_num: parent_page,
                child: old_child,
            })
        }
    }

    let cell_bytes: Vec<Vec<u8>> = entries
        .iter()
        .map(|(child, key)| build_interior_cell(*child, *key))
        .collect();
    let buf = pager.get_page_mut(parent_page)?;
    write_interior_page(buf, header_start, parent_page, &cell_bytes, rightmost)
}

/// The root page number can never change, so collapsing the root's sole
/// remaining child (`only_child`) means relocating that child's content
/// (leaf or interior, verbatim) into the root page in place, then
/// deallocating `only_child`'s now-vacated page. Mirrors
/// `insert.rs::root_split` in reverse.
fn collapse_root(
    pager: &mut Pager,
    usable_size: u32,
    root_page: u32,
    only_child: u32,
) -> Result<(), BtreeError> {
    let child_header_start = page1_header_start(only_child);
    let content = pager.get_page_mut(only_child)?.clone();
    let page_type = read_page_type(&content, child_header_start, only_child)?;
    let root_header_start = page1_header_start(root_page);

    match page_type {
        LEAF_TABLE => {
            let cells = collect_leaf_cells(&content, child_header_start, only_child, usable_size)?;
            let dest = pager.get_page_mut(root_page)?;
            write_leaf_page(dest, root_header_start, root_page, &cell_bytes(cells))?;
        }
        INTERIOR_TABLE => {
            let (entries, rightmost) =
                collect_interior_entries(&content, child_header_start, only_child)?;
            let cells: Vec<Vec<u8>> = entries
                .iter()
                .map(|(child, key)| build_interior_cell(*child, *key))
                .collect();
            let dest = pager.get_page_mut(root_page)?;
            write_interior_page(dest, root_header_start, root_page, &cells, rightmost)?;
        }
        other => {
            return Err(BtreeError::UnexpectedPageType {
                page_num: only_child,
                page_type: other,
            })
        }
    }

    Ok(pager.deallocate_page(only_child)?)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use crate::btree::insert_row;
    use std::path::Path;

    use crate::btree::test_minimal_db as minimal_db;

    #[test]
    fn deleting_a_missing_rowid_errors() {
        let page_size = 512u32;
        let (vfs, header) = minimal_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();

        insert_row(&mut pager, &header, 1, 1, b"hello").unwrap();
        let err = delete_row(&mut pager, &header, 1, 2).unwrap_err();
        assert!(matches!(err, BtreeError::RowidNotFound { rowid: 2 }));
    }

    #[test]
    fn deleting_the_only_row_leaves_an_empty_root_leaf() {
        let page_size = 512u32;
        let (vfs, header) = minimal_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();

        insert_row(&mut pager, &header, 1, 1, b"hello").unwrap();
        delete_row(&mut pager, &header, 1, 1).unwrap();

        let header_start = page1_header_start(1);
        let buf = pager.get_page_mut(1).unwrap().clone();
        let page_type = read_page_type(&buf, header_start, 1).unwrap();
        assert_eq!(page_type, LEAF_TABLE);
        let cells = collect_leaf_cells(&buf, header_start, 1, header.usable_page_size()).unwrap();
        assert!(cells.is_empty());
    }

    #[test]
    fn deleting_one_of_two_rows_keeps_the_other() {
        let page_size = 512u32;
        let (vfs, header) = minimal_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();

        insert_row(&mut pager, &header, 1, 1, b"hello").unwrap();
        insert_row(&mut pager, &header, 1, 2, b"world").unwrap();
        delete_row(&mut pager, &header, 1, 1).unwrap();

        let header_start = page1_header_start(1);
        let buf = pager.get_page_mut(1).unwrap().clone();
        let cells = collect_leaf_cells(&buf, header_start, 1, header.usable_page_size()).unwrap();
        assert_eq!(cells.len(), 1);
        assert_eq!(cells[0].0, 2);
    }

    fn freelist_page_count(pager: &mut Pager) -> u32 {
        let page1 = pager.get_page_mut(1).unwrap().clone();
        u32::from_be_bytes(page1[36..40].try_into().unwrap())
    }

    /// #52 tagged MC/DC vector (obligation `delete_61`, decision
    /// `cells.len() > 1 || ancestors.is_empty()`): leaf A
    /// (`cells.len() > 1`) true, leaf B (`ancestors.is_empty()`) false —
    /// a multi-page tree where the leaf being deleted from still has
    /// other rows left, so it splices in place rather than collapsing.
    /// Independence pair for A against
    /// `mcdc__delete_61__v2_last_cell_in_leaf_with_ancestors_collapses`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__delete_61__v1_leaf_survives_with_ancestors() {
        let page_size = 512u32;
        let (vfs, header) = minimal_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();

        let filler = "x".repeat(190);
        let n = 60i64;
        for i in 1..=n {
            insert_row(
                &mut pager,
                &header,
                1,
                i,
                format!("{filler}-{i:04}").as_bytes(),
            )
            .unwrap();
        }
        // Root has split into an interior page (ancestors non-empty for
        // every leaf); the last-inserted rowid's leaf still holds other
        // rows after this single delete.
        delete_row(&mut pager, &header, 1, n).unwrap();
        assert!(delete_row(&mut pager, &header, 1, n - 1).is_ok());
    }

    /// #52 tagged MC/DC vector (obligation `delete_61`): both leaves
    /// false — a multi-page tree where the leaf being deleted from holds
    /// exactly one cell, so it must be deallocated and its removal
    /// cascaded into ancestors rather than spliced in place. Independence
    /// pair for A against `mcdc__delete_61__v1_leaf_survives_with_ancestors`
    /// and for B against
    /// `mcdc__delete_61__v3_only_row_in_root_leaf_has_no_ancestors`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__delete_61__v2_last_cell_in_leaf_with_ancestors_collapses() {
        let page_size = 512u32;
        let (vfs, header) = minimal_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();

        let filler = "x".repeat(190);
        let n = 60i64;
        for i in 1..=n {
            insert_row(
                &mut pager,
                &header,
                1,
                i,
                format!("{filler}-{i:04}").as_bytes(),
            )
            .unwrap();
        }
        // Draining every row but the last from this multi-page tree
        // leaves exactly one leaf with exactly one cell and non-empty
        // ancestors; deleting it must collapse the leaf.
        let before = freelist_page_count(&mut pager);
        for i in 1..n {
            delete_row(&mut pager, &header, 1, i).unwrap();
        }
        delete_row(&mut pager, &header, 1, n).unwrap();
        let after = freelist_page_count(&mut pager);
        assert!(
            after > before,
            "emptying the last leaf in a multi-page tree must free at least one page"
        );
    }

    /// #52 tagged MC/DC vector (obligation `delete_61`): leaf A false,
    /// leaf B (`ancestors.is_empty()`) true independently flips the
    /// outcome to true — the single-page root-leaf case, where the
    /// (empty) root can never be collapsed away. Independence pair for B
    /// against `mcdc__delete_61__v2_last_cell_in_leaf_with_ancestors_collapses`.
    #[test]
    #[allow(non_snake_case)]
    fn mcdc__delete_61__v3_only_row_in_root_leaf_has_no_ancestors() {
        let page_size = 512u32;
        let (vfs, header) = minimal_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();

        insert_row(&mut pager, &header, 1, 1, b"hello").unwrap();
        assert!(delete_row(&mut pager, &header, 1, 1).is_ok());
    }

    /// Kills mutants that turn `cell_overflow_page`/`free_overflow_chain`
    /// into no-ops (`Ok(0)` / `Ok(())`): a deleted row whose payload
    /// actually spilled to overflow pages must return those pages to the
    /// freelist, not silently leak them.
    #[test]
    fn deleting_a_row_with_overflow_frees_its_overflow_chain() {
        let page_size = 512u32;
        let (vfs, header) = minimal_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();

        // max_local for a 512-byte page is 512 - 35 = 477, so this payload
        // forces at least one overflow page.
        let payload = vec![b'x'; 1000];
        insert_row(&mut pager, &header, 1, 1, &payload).unwrap();
        assert_eq!(freelist_page_count(&mut pager), 0, "no pages freed yet");

        delete_row(&mut pager, &header, 1, 1).unwrap();

        assert!(
            freelist_page_count(&mut pager) > 0,
            "the row's overflow chain must be returned to the freelist on delete"
        );
    }

    /// Kills the `!` deletion mutant in `free_overflow_chain`'s cycle
    /// guard (`!visited.insert(page_num)`): a well-formed, non-cyclic
    /// chain must free cleanly, not error out on its very first page.
    #[test]
    fn free_overflow_chain_frees_every_page_in_a_well_formed_chain() {
        let page_size = 512u32;
        let (vfs, header) = minimal_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();
        let _ = &header;

        let p2 = pager.allocate_page().unwrap();
        let p3 = pager.allocate_page().unwrap();
        let p4 = pager.allocate_page().unwrap();
        {
            let buf = pager.get_page_mut(p2).unwrap();
            buf[0..4].copy_from_slice(&p3.to_be_bytes());
        }
        {
            let buf = pager.get_page_mut(p3).unwrap();
            buf[0..4].copy_from_slice(&p4.to_be_bytes());
        }
        {
            let buf = pager.get_page_mut(p4).unwrap();
            buf[0..4].copy_from_slice(&0u32.to_be_bytes());
        }
        let before = freelist_page_count(&mut pager);

        free_overflow_chain(&mut pager, p2).unwrap();

        assert_eq!(
            freelist_page_count(&mut pager),
            before + 3,
            "every page in the 3-page chain must be freed"
        );
    }

    /// Regression guard for a real bug found while implementing the
    /// index b-tree delete path (#171): when an interior page drains to
    /// zero routing entries, its surviving `rightmost` child (which may
    /// hold real, live rows entirely unrelated to whatever just emptied)
    /// must be spliced into the interior page's own parent — not dropped
    /// by recursing into `collapse_into_ancestors` as if the whole
    /// interior page (rightmost included) had emptied. Small pages with
    /// a big-enough spread of rowids force cascading interior splits
    /// down to single-entry interior pages; deleting everything under
    /// one such entry's child (draining it) while its sibling
    /// `rightmost` subtree stays alive reproduces the scenario.
    #[test]
    fn deleting_one_subtree_never_orphans_a_sibling_rightmost_subtree() {
        let page_size = 512u32;
        let (vfs, header) = minimal_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();

        let filler = "x".repeat(190);
        let n = 60i64;
        for i in 1..=n {
            insert_row(
                &mut pager,
                &header,
                1,
                i,
                format!("{filler}-{i:04}").as_bytes(),
            )
            .unwrap();
        }

        // Delete the lowest half of rowids — on a small enough page size
        // this drains at least one interior page down to a single
        // routing entry and then to zero, exercising the splice path,
        // while the upper half's rows must all remain reachable.
        for i in 1..=(n / 2) {
            delete_row(&mut pager, &header, 1, i).unwrap();
        }

        let usable_size = header.usable_page_size();
        let mut remaining = Vec::new();
        let mut stack = vec![1u32];
        while let Some(page_num) = stack.pop() {
            let header_start = page1_header_start(page_num);
            let buf = pager.get_page_mut(page_num).unwrap().clone();
            let page_type = read_page_type(&buf, header_start, page_num).unwrap();
            if page_type == LEAF_TABLE {
                let cells = collect_leaf_cells(&buf, header_start, page_num, usable_size).unwrap();
                remaining.extend(cells.into_iter().map(|(rowid, _)| rowid));
            } else {
                let (entries, rightmost) =
                    collect_interior_entries(&buf, header_start, page_num).unwrap();
                stack.push(rightmost);
                stack.extend(entries.iter().map(|(child, _)| *child));
            }
        }
        remaining.sort_unstable();
        let expected: Vec<i64> = (n / 2 + 1..=n).collect();
        assert_eq!(
            remaining, expected,
            "every surviving row must still be reachable from the root — none orphaned"
        );
    }

    #[test]
    fn free_overflow_chain_detects_a_cycle() {
        let page_size = 512u32;
        let (vfs, _header) = minimal_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();

        let page_a = pager.allocate_page().unwrap();
        let page_b = pager.allocate_page().unwrap();
        {
            let buf = pager.get_page_mut(page_a).unwrap();
            buf[0..4].copy_from_slice(&page_b.to_be_bytes());
        }
        {
            let buf = pager.get_page_mut(page_b).unwrap();
            buf[0..4].copy_from_slice(&page_a.to_be_bytes());
        }

        let err = free_overflow_chain(&mut pager, page_a).unwrap_err();
        assert!(matches!(err, BtreeError::OverflowChainCycle { .. }));
    }

    #[test]
    fn collapse_into_ancestors_errors_with_no_ancestors() {
        let page_size = 512u32;
        let (vfs, header) = minimal_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();
        let usable_size = header.usable_page_size();

        let err = collapse_into_ancestors(&mut pager, usable_size, 1, &[], 1).unwrap_err();
        assert!(matches!(err, BtreeError::Internal(_)));
    }

    #[test]
    fn collapse_into_ancestors_errors_when_rightmost_emptied_with_no_routing_entries() {
        let page_size = 512u32;
        let (vfs, header) = minimal_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();
        let usable_size = header.usable_page_size();

        let parent = pager.allocate_page().unwrap();
        let emptied = pager.allocate_page().unwrap();
        {
            let buf = pager.get_page_mut(parent).unwrap();
            write_interior_page(buf, 0, parent, &[], emptied).unwrap();
        }

        let err =
            collapse_into_ancestors(&mut pager, usable_size, 1, &[parent], emptied).unwrap_err();
        assert!(matches!(err, BtreeError::Internal(_)));
    }

    #[test]
    fn collapse_into_ancestors_errors_when_child_route_missing() {
        let page_size = 512u32;
        let (vfs, header) = minimal_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();
        let usable_size = header.usable_page_size();

        let parent = pager.allocate_page().unwrap();
        let other_child = pager.allocate_page().unwrap();
        let unrelated_page = 999u32;
        {
            let buf = pager.get_page_mut(parent).unwrap();
            write_interior_page(
                buf,
                0,
                parent,
                &[build_interior_cell(other_child, 5)],
                other_child,
            )
            .unwrap();
        }

        let err = collapse_into_ancestors(&mut pager, usable_size, 1, &[parent], unrelated_page)
            .unwrap_err();
        assert!(matches!(err, BtreeError::MissingChildRoute { .. }));
    }

    #[test]
    fn collapse_into_ancestors_collapses_the_root_when_it_drains_to_one_child() {
        let page_size = 512u32;
        let (vfs, header) = minimal_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();
        let usable_size = header.usable_page_size();

        let child_a = pager.allocate_page().unwrap();
        let child_b = pager.allocate_page().unwrap();
        {
            let buf = pager.get_page_mut(child_b).unwrap();
            write_leaf_page(buf, 0, child_b, &[]).unwrap();
        }
        insert_row(&mut pager, &header, child_b, 42, b"payload").unwrap();

        {
            let header_start = page1_header_start(1);
            let buf = pager.get_page_mut(1).unwrap();
            write_interior_page(
                buf,
                header_start,
                1,
                &[build_interior_cell(child_a, 10)],
                child_b,
            )
            .unwrap();
        }

        collapse_into_ancestors(&mut pager, usable_size, 1, &[1], child_a).unwrap();

        let header_start = page1_header_start(1);
        let buf = pager.get_page_mut(1).unwrap().clone();
        let page_type = read_page_type(&buf, header_start, 1).unwrap();
        assert_eq!(page_type, LEAF_TABLE);
        let cells = collect_leaf_cells(&buf, header_start, 1, usable_size).unwrap();
        assert_eq!(cells.len(), 1);
        assert_eq!(cells[0].0, 42);
    }

    #[test]
    fn collapse_into_ancestors_cascades_through_a_non_root_parent() {
        let page_size = 512u32;
        let (vfs, header) = minimal_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();
        let usable_size = header.usable_page_size();

        let mid_page = pager.allocate_page().unwrap();
        let emptied_child = pager.allocate_page().unwrap();
        let leaf_x = pager.allocate_page().unwrap();
        let other_root_child = pager.allocate_page().unwrap();

        {
            let buf = pager.get_page_mut(mid_page).unwrap();
            write_interior_page(
                buf,
                0,
                mid_page,
                &[build_interior_cell(emptied_child, 10)],
                leaf_x,
            )
            .unwrap();
        }
        {
            let header_start = page1_header_start(1);
            let buf = pager.get_page_mut(1).unwrap();
            write_interior_page(
                buf,
                header_start,
                1,
                &[build_interior_cell(mid_page, 50)],
                other_root_child,
            )
            .unwrap();
        }

        collapse_into_ancestors(&mut pager, usable_size, 1, &[1, mid_page], emptied_child).unwrap();

        let header_start = page1_header_start(1);
        let buf = pager.get_page_mut(1).unwrap().clone();
        let (entries, rightmost) = collect_interior_entries(&buf, header_start, 1).unwrap();
        assert_eq!(entries, vec![(leaf_x, 50)]);
        assert_eq!(rightmost, other_root_child);
    }

    #[test]
    fn collapse_root_relocates_an_interior_only_child() {
        let page_size = 512u32;
        let (vfs, header) = minimal_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();
        let usable_size = header.usable_page_size();

        let only_child = pager.allocate_page().unwrap();
        let grandchild_a = pager.allocate_page().unwrap();
        let grandchild_rightmost = 77u32;
        {
            let buf = pager.get_page_mut(only_child).unwrap();
            write_interior_page(
                buf,
                0,
                only_child,
                &[build_interior_cell(grandchild_a, 5)],
                grandchild_rightmost,
            )
            .unwrap();
        }

        collapse_root(&mut pager, usable_size, 1, only_child).unwrap();

        let header_start = page1_header_start(1);
        let buf = pager.get_page_mut(1).unwrap().clone();
        let page_type = read_page_type(&buf, header_start, 1).unwrap();
        assert_eq!(page_type, INTERIOR_TABLE);
        let (entries, rightmost) = collect_interior_entries(&buf, header_start, 1).unwrap();
        assert_eq!(entries, vec![(grandchild_a, 5)]);
        assert_eq!(rightmost, grandchild_rightmost);
    }

    #[test]
    fn collapse_root_rejects_an_unexpected_child_page_type() {
        let page_size = 512u32;
        let (vfs, header) = minimal_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();
        let usable_size = header.usable_page_size();

        let only_child = pager.allocate_page().unwrap();
        {
            let buf = pager.get_page_mut(only_child).unwrap();
            buf[0] = 0xFF;
        }

        let err = collapse_root(&mut pager, usable_size, 1, only_child).unwrap_err();
        assert!(matches!(err, BtreeError::UnexpectedPageType { .. }));
    }

    #[test]
    fn splice_child_errors_with_no_ancestors() {
        let page_size = 512u32;
        let (vfs, _header) = minimal_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();

        let err = splice_child(&mut pager, &[], 5, 6).unwrap_err();
        assert!(matches!(err, BtreeError::Internal(_)));
    }

    #[test]
    fn splice_child_errors_when_old_child_route_missing() {
        let page_size = 512u32;
        let (vfs, _header) = minimal_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();

        let parent = pager.allocate_page().unwrap();
        let other_child = pager.allocate_page().unwrap();
        {
            let buf = pager.get_page_mut(parent).unwrap();
            write_interior_page(
                buf,
                0,
                parent,
                &[build_interior_cell(other_child, 1)],
                other_child,
            )
            .unwrap();
        }

        let err = splice_child(&mut pager, &[parent], 999, 1000).unwrap_err();
        assert!(matches!(err, BtreeError::MissingChildRoute { .. }));
    }

    #[test]
    fn splice_child_replaces_an_entry_reference() {
        let page_size = 512u32;
        let (vfs, _header) = minimal_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();

        let parent = pager.allocate_page().unwrap();
        let old_child = pager.allocate_page().unwrap();
        let rightmost = pager.allocate_page().unwrap();
        let new_child = 999u32;
        {
            let buf = pager.get_page_mut(parent).unwrap();
            write_interior_page(
                buf,
                0,
                parent,
                &[build_interior_cell(old_child, 7)],
                rightmost,
            )
            .unwrap();
        }

        splice_child(&mut pager, &[parent], old_child, new_child).unwrap();

        let buf = pager.get_page_mut(parent).unwrap().clone();
        let (entries, rm) = collect_interior_entries(&buf, 0, parent).unwrap();
        assert_eq!(entries, vec![(new_child, 7)]);
        assert_eq!(rm, rightmost);
    }

    #[test]
    fn splice_child_replaces_a_rightmost_reference() {
        let page_size = 512u32;
        let (vfs, _header) = minimal_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();

        let parent = pager.allocate_page().unwrap();
        let entry_child = pager.allocate_page().unwrap();
        let old_rightmost = pager.allocate_page().unwrap();
        let new_rightmost = 12345u32;
        {
            let buf = pager.get_page_mut(parent).unwrap();
            write_interior_page(
                buf,
                0,
                parent,
                &[build_interior_cell(entry_child, 3)],
                old_rightmost,
            )
            .unwrap();
        }

        splice_child(&mut pager, &[parent], old_rightmost, new_rightmost).unwrap();

        let buf = pager.get_page_mut(parent).unwrap().clone();
        let (entries, rightmost) = collect_interior_entries(&buf, 0, parent).unwrap();
        assert_eq!(entries, vec![(entry_child, 3)]);
        assert_eq!(rightmost, new_rightmost);
    }
}
