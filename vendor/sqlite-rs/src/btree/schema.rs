// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! Runtime write-path helpers for `CREATE TABLE`/`CREATE INDEX` (#215):
//! allocating a fresh, empty b-tree root page for a new table or index,
//! and (for `CREATE INDEX` on an already-populated table) building index
//! entries for every pre-existing row. `DROP TABLE`/`DROP INDEX` reuse
//! `super::free_btree_pages` plus `master::delete_master_row` directly —
//! nothing DROP-specific belongs in this module.

use super::index::write_index_leaf_page;
use super::{page1_header_start, write_leaf_page, BtreeError};
use crate::header::DatabaseHeader;
use crate::pager::Pager;
use crate::record::{decode_record, Value};

/// Allocates a page and initializes it as an empty table-b-tree leaf,
/// returning its page number — the root of a brand-new `CREATE TABLE`.
pub fn create_empty_table_root(pager: &mut Pager) -> Result<u32, BtreeError> {
    let root_page = pager.allocate_page()?;
    let header_start = page1_header_start(root_page);
    let buf = pager.get_page_mut(root_page)?;
    write_leaf_page(buf, header_start, root_page, &[])?;
    Ok(root_page)
}

/// Allocates a page and initializes it as an empty index-b-tree leaf,
/// returning its page number — the root of a brand-new `CREATE INDEX`
/// (before any pre-existing rows are populated into it).
pub fn create_empty_index_root(pager: &mut Pager) -> Result<u32, BtreeError> {
    let root_page = pager.allocate_page()?;
    let header_start = page1_header_start(root_page);
    let buf = pager.get_page_mut(root_page)?;
    write_index_leaf_page(buf, header_start, root_page, &[])?;
    Ok(root_page)
}

/// Populates a freshly-created index (rooted at `index_root_page`) with
/// one entry per existing row of the table rooted at `table_root_page`,
/// mirroring the on-disk index-key convention used by ordinary DML index
/// maintenance (`codegen::index_maintenance::emit_index_key_ops`):
/// indexed column values in `column_indices` order, then the row's rowid
/// last.
pub fn populate_index_from_table(
    pager: &mut Pager,
    header: &DatabaseHeader,
    table_root_page: u32,
    index_root_page: u32,
    column_indices: &[usize],
) -> Result<(), BtreeError> {
    let encoding = header.text_encoding;
    // Collect every row's key up front rather than interleaving reads
    // with `insert_entry` writes: the table cursor borrows `pager`
    // immutably for its whole traversal, and `insert_entry` needs `pager`
    // mutably (it may split/allocate index pages), so the two can't run
    // interleaved against the same `&mut Pager`.
    let mut keys: Vec<Vec<Value>> = Vec::new();
    {
        let mut cursor = crate::btree::TableCursor::new(&*pager, header, table_root_page);
        let mut row = cursor.first_row()?;
        while let Some(r) = row {
            let values = decode_record(&r.payload, encoding)?;
            let mut key: Vec<Value> = Vec::with_capacity(column_indices.len().saturating_add(1));
            for &idx in column_indices {
                key.push(values.get(idx).cloned().unwrap_or(Value::Null));
            }
            key.push(Value::Integer(r.rowid));
            keys.push(key);
            row = cursor.next_row()?;
        }
    }
    for key in &keys {
        super::insert_entry(pager, header, index_root_page, key, encoding)?;
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::btree::{insert_row, MasterEntry};
    use crate::record::{decode_record, encode_record, TextEncoding, Value};
    use std::path::Path;

    use super::super::test_minimal_db as minimal_db;

    #[test]
    fn create_empty_table_root_is_a_readable_empty_leaf() {
        let page_size = 512u32;
        let (vfs, header) = minimal_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();

        let root = create_empty_table_root(&mut pager).unwrap();
        assert_ne!(root, 1);

        let mut cursor = crate::btree::TableCursor::new(&pager, &header, root);
        assert!(cursor.first().unwrap().is_none());
    }

    #[test]
    fn create_empty_index_root_is_a_readable_empty_leaf() {
        let page_size = 512u32;
        let (vfs, header) = minimal_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();

        let root = create_empty_index_root(&mut pager).unwrap();
        assert_ne!(root, 1);

        let mut cursor = crate::btree::IndexCursor::new(&pager, header.usable_page_size(), root);
        assert!(cursor.first().unwrap().is_none());
    }

    #[test]
    fn populate_index_from_table_covers_existing_rows() {
        let page_size = 512u32;
        let (vfs, header) = minimal_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();

        let table_root = create_empty_table_root(&mut pager).unwrap();
        for (rowid, name) in [(1i64, "a"), (2, "b"), (3, "c")] {
            let payload =
                encode_record(&[Value::Text(name.to_string().into())], TextEncoding::Utf8);
            insert_row(&mut pager, &header, table_root, rowid, &payload).unwrap();
        }
        // Keep sqlite_master's own single row so `insert_master_row`
        // conventions aren't disturbed by this fixture — not needed by
        // this test, which exercises `populate_index_from_table` in
        // isolation against a bare table b-tree.
        let _ = MasterEntry {
            kind: "table".to_string(),
            name: "t".to_string(),
            tbl_name: "t".to_string(),
            rootpage: table_root,
            sql: "CREATE TABLE t(name)".to_string(),
        };

        let index_root = create_empty_index_root(&mut pager).unwrap();
        populate_index_from_table(&mut pager, &header, table_root, index_root, &[0]).unwrap();

        let mut cursor =
            crate::btree::IndexCursor::new(&pager, header.usable_page_size(), index_root);
        let mut seen = Vec::new();
        let mut row = cursor.first().unwrap();
        while let Some(r) = row {
            seen.push(decode_record(&r.payload, TextEncoding::Utf8).unwrap());
            row = cursor.next().unwrap();
        }
        assert_eq!(seen.len(), 3);
    }
}
