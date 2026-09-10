// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! `sqlite_master` (and `sqlite_sequence`) write-path helpers for DDL
//! execution: schema cookie bump, master-row insert/delete, and
//! AUTOINCREMENT sequence tracking (#193). Lives inside `src/btree/`
//! (a sibling of `insert.rs`/`delete.rs`) so it can reuse this module's
//! private page-layout helpers (`write_leaf_page`, `page1_header_start`)
//! the same way those two do — see `insert.rs`'s module doc.
//!
//! No general header serializer exists yet (#167's documented gap,
//! `src/pager.rs`'s `PAGE_COUNT_OFFSET` comment) — the schema cookie is
//! patched in place on page 1 at its fixed byte offset (40), matching
//! that precedent rather than building one for a single field.
//!
//! This module only provides the write primitives; wiring them to actual
//! `CREATE`/`DROP`/`INSERT` statement execution is VDBE write-opcode
//! scope (#194), not this ticket's.

use super::{page1_header_start, write_leaf_page, BtreeError};
use crate::header::DatabaseHeader;
use crate::pager::Pager;
use crate::record::{decode_record, encode_record, TextEncoding, Value};

/// The fixed root page of `sqlite_master` (SQLite file-format convention:
/// always page 1).
pub const SQLITE_MASTER_ROOT_PAGE: u32 = 1;

/// Byte offset of the schema cookie in the 100-byte database header
/// (bytes 40-43) — see [`crate::header::DatabaseHeader::schema_cookie`].
const SCHEMA_COOKIE_OFFSET: usize = 40;

/// Reads the schema cookie directly off page 1's current bytes (which may
/// already be dirty from an earlier write in the same statement), rather
/// than trusting a possibly-stale `DatabaseHeader` snapshot.
fn read_schema_cookie(pager: &mut Pager) -> Result<u32, BtreeError> {
    let page1 = pager.get_page_mut(SQLITE_MASTER_ROOT_PAGE)?;
    let bytes: [u8; 4] = page1
        .get(SCHEMA_COOKIE_OFFSET..SCHEMA_COOKIE_OFFSET + 4)
        .ok_or(BtreeError::PageTooShort {
            page_num: SQLITE_MASTER_ROOT_PAGE,
            len: page1.len(),
        })?
        .try_into()
        .map_err(|_| BtreeError::Internal("schema cookie slice was not 4 bytes"))?;
    Ok(u32::from_be_bytes(bytes))
}

/// Increments the schema cookie in the database header and writes it
/// back, returning the new value. Every schema-mutating statement (CREATE
/// TABLE/INDEX, DROP TABLE/INDEX) calls this once.
///
/// Wraps on overflow rather than saturating, matching stock SQLite's own
/// `u32` schema-cookie wraparound behavior.
pub fn bump_schema_cookie(pager: &mut Pager) -> Result<u32, BtreeError> {
    let new_cookie = read_schema_cookie(pager)?.wrapping_add(1);
    let page1 = pager.get_page_mut(SQLITE_MASTER_ROOT_PAGE)?;
    let len = page1.len();
    page1
        .get_mut(SCHEMA_COOKIE_OFFSET..SCHEMA_COOKIE_OFFSET + 4)
        .ok_or(BtreeError::PageTooShort {
            page_num: SQLITE_MASTER_ROOT_PAGE,
            len,
        })?
        .copy_from_slice(&new_cookie.to_be_bytes());
    Ok(new_cookie)
}

/// One `sqlite_master` (or `sqlite_sequence`) row's column values, in the
/// schema's fixed column order: `type, name, tbl_name, rootpage, sql`.
#[derive(Debug, Clone)]
pub struct MasterEntry {
    /// The schema object's type: `"table"`, `"index"`, `"view"`, or `"trigger"`.
    pub kind: String,
    /// The schema object's own name.
    pub name: String,
    /// The table this object applies to (equal to `name` for a table itself).
    pub tbl_name: String,
    /// Root page number of the object's b-tree, or 0 when it has none.
    pub rootpage: u32,
    /// The `CREATE ...` SQL text that defines the object.
    pub sql: String,
}

/// Scans the table b-tree rooted at `root_page` and returns the highest
/// rowid present, or `None` if the table is empty. Used to allocate the
/// next rowid for a `sqlite_master`/`sqlite_sequence` insert, mirroring
/// SQLite's default (no explicit rowid given) rowid assignment.
fn max_rowid(
    pager: &mut Pager,
    header: &DatabaseHeader,
    root_page: u32,
) -> Result<Option<i64>, BtreeError> {
    // Rowid-only scan: never touches column payloads, so it uses the
    // lean, position-only `first`/`next` directly (#473) instead of
    // `first_row`/`next_row` — no payload reassembly happens per row.
    let mut cursor = crate::btree::TableCursor::new(&*pager, header, root_page);
    let mut max = None;
    let mut row = cursor.first()?;
    while let Some(rowid) = row {
        max = Some(max.map_or(rowid, |m: i64| m.max(rowid)));
        row = cursor.next()?;
    }
    Ok(max)
}

/// Finds the rowid of the row in the table b-tree rooted at `root_page`
/// whose column at `key_column` (0-based) decodes to the text `key`, or
/// `None` if no such row exists.
fn find_rowid_by_text_column(
    pager: &mut Pager,
    header: &DatabaseHeader,
    root_page: u32,
    key_column: usize,
    key: &str,
    encoding: TextEncoding,
) -> Result<Option<i64>, BtreeError> {
    let mut cursor = crate::btree::TableCursor::new(&*pager, header, root_page);
    let mut row = cursor.first_row()?;
    while let Some(r) = row {
        let values = decode_record(&r.payload, encoding)?;
        if let Some(Value::Text(s)) = values.get(key_column) {
            if s.as_ref() == key {
                return Ok(Some(r.rowid));
            }
        }
        row = cursor.next_row()?;
    }
    Ok(None)
}

/// Inserts one row into `sqlite_master` for a newly created table or
/// index (`CREATE TABLE`/`CREATE INDEX`). Does not bump the schema
/// cookie — callers do that once per statement via
/// [`bump_schema_cookie`].
pub fn insert_master_row(
    pager: &mut Pager,
    header: &DatabaseHeader,
    entry: &MasterEntry,
) -> Result<(), BtreeError> {
    let next_rowid = max_rowid(pager, header, SQLITE_MASTER_ROOT_PAGE)?
        .unwrap_or(0)
        .saturating_add(1);
    let values = [
        Value::Text(entry.kind.as_str().into()),
        Value::Text(entry.name.as_str().into()),
        Value::Text(entry.tbl_name.as_str().into()),
        Value::Integer(entry.rootpage as i64),
        Value::Text(entry.sql.as_str().into()),
    ];
    let payload = encode_record(&values, header.text_encoding);
    super::insert_row(pager, header, SQLITE_MASTER_ROOT_PAGE, next_rowid, &payload)
}

/// Deletes the `sqlite_master` row named `name` (`DROP TABLE`/`DROP
/// INDEX`). Does not bump the schema cookie — see [`insert_master_row`].
/// Returns `Err(BtreeError::MasterEntryNotFound)` if no such row exists.
pub fn delete_master_row(
    pager: &mut Pager,
    header: &DatabaseHeader,
    name: &str,
) -> Result<(), BtreeError> {
    let rowid = find_rowid_by_text_column(
        pager,
        header,
        SQLITE_MASTER_ROOT_PAGE,
        1,
        name,
        header.text_encoding,
    )?
    .ok_or_else(|| BtreeError::MasterEntryNotFound {
        name: Box::leak(name.to_string().into_boxed_str()),
    })?;
    super::delete_row(pager, header, SQLITE_MASTER_ROOT_PAGE, rowid)
}

/// Canonical `sqlite_sequence` DDL text, matching stock SQLite's
/// auto-created definition verbatim (so the `sql` column round-trips
/// identically when read back by stock `sqlite3`).
const SQLITE_SEQUENCE_SQL: &str = "CREATE TABLE sqlite_sequence(name,seq)";

/// Returns `sqlite_sequence`'s root page, creating the table (allocating
/// a page, initializing it as an empty leaf, and registering it in
/// `sqlite_master`, bumping the schema cookie) on first use — mirroring
/// stock SQLite's "auto-created on first AUTOINCREMENT table" behavior.
pub fn ensure_sqlite_sequence_table(
    pager: &mut Pager,
    header: &DatabaseHeader,
) -> Result<u32, BtreeError> {
    if let Some(existing) = find_master_rootpage(pager, header, "sqlite_sequence")? {
        return Ok(existing);
    }

    let root_page = pager.allocate_page()?;
    let header_start = page1_header_start(root_page);
    let buf = pager.get_page_mut(root_page)?;
    write_leaf_page(buf, header_start, root_page, &[])?;

    insert_master_row(
        pager,
        header,
        &MasterEntry {
            kind: "table".to_string(),
            name: "sqlite_sequence".to_string(),
            tbl_name: "sqlite_sequence".to_string(),
            rootpage: root_page,
            sql: SQLITE_SEQUENCE_SQL.to_string(),
        },
    )?;
    bump_schema_cookie(pager)?;

    Ok(root_page)
}

/// Looks up a table/index's root page by name in `sqlite_master`, without
/// requiring a full [`crate::schema::read_schema`] pass.
fn find_master_rootpage(
    pager: &mut Pager,
    header: &DatabaseHeader,
    name: &str,
) -> Result<Option<u32>, BtreeError> {
    let mut cursor = crate::btree::TableCursor::new(&*pager, header, SQLITE_MASTER_ROOT_PAGE);
    let mut row = cursor.first_row()?;
    while let Some(r) = row {
        let values = decode_record(&r.payload, header.text_encoding)?;
        if let (Some(Value::Text(n)), Some(Value::Integer(rp))) = (values.get(1), values.get(3)) {
            if n.as_ref() == name {
                let rootpage = u32::try_from(*rp).map_err(|_| BtreeError::InvalidRootPage {
                    name: Box::leak(name.to_string().into_boxed_str()),
                    rootpage: *rp,
                })?;
                if rootpage == 0 {
                    return Err(BtreeError::InvalidRootPage {
                        name: Box::leak(name.to_string().into_boxed_str()),
                        rootpage: *rp,
                    });
                }
                return Ok(Some(rootpage));
            }
        }
        row = cursor.next_row()?;
    }
    Ok(None)
}

/// Updates `sqlite_sequence` for `table_name` after an INSERT assigns it
/// `rowid`: creates the sequence table if needed (only ever necessary for
/// an AUTOINCREMENT table), inserts a new `(table_name, rowid)` row if
/// none exists yet, or bumps the existing row's `seq` to `rowid` when
/// `rowid` exceeds the tracked maximum. No-op when `rowid` does not
/// exceed the tracked value (mirrors SQLite: `sqlite_sequence.seq` only
/// ever grows).
pub fn update_sequence(
    pager: &mut Pager,
    header: &DatabaseHeader,
    table_name: &str,
    rowid: i64,
) -> Result<(), BtreeError> {
    let seq_root = ensure_sqlite_sequence_table(pager, header)?;

    let mut cursor = crate::btree::TableCursor::new(&*pager, header, seq_root);
    let mut existing: Option<(i64, i64)> = None; // (row rowid, current seq)
    let mut row = cursor.first_row()?;
    while let Some(r) = row {
        let values = decode_record(&r.payload, header.text_encoding)?;
        if let (Some(Value::Text(n)), Some(Value::Integer(seq))) = (values.first(), values.get(1)) {
            if n.as_ref() == table_name {
                existing = Some((r.rowid, *seq));
                break;
            }
        }
        row = cursor.next_row()?;
    }

    match existing {
        None => {
            let next_rowid = max_rowid(pager, header, seq_root)?
                .unwrap_or(0)
                .saturating_add(1);
            let values = [Value::Text(table_name.into()), Value::Integer(rowid)];
            let payload = encode_record(&values, header.text_encoding);
            super::insert_row(pager, header, seq_root, next_rowid, &payload)?;
        }
        Some((row_rowid, current_seq)) if rowid > current_seq => {
            super::delete_row(pager, header, seq_root, row_rowid)?;
            let values = [Value::Text(table_name.into()), Value::Integer(rowid)];
            let payload = encode_record(&values, header.text_encoding);
            super::insert_row(pager, header, seq_root, row_rowid, &payload)?;
        }
        Some(_) => {}
    }

    Ok(())
}

/// Canonical `sqlite_stat1` DDL text, matching stock SQLite's own
/// definition (`sqlite3 src/analyze.c`) verbatim so the `sql` column
/// round-trips identically when read back by stock `sqlite3`.
const SQLITE_STAT1_SQL: &str = "CREATE TABLE sqlite_stat1(tbl,idx,stat)";

/// Returns `sqlite_stat1`'s root page, creating the table (allocating a
/// page, initializing it as an empty leaf, and registering it in
/// `sqlite_master`, bumping the schema cookie) on first use — mirrors
/// [`ensure_sqlite_sequence_table`]'s "auto-created on first use"
/// pattern; `ANALYZE` (#461) is `sqlite_stat1`'s equivalent trigger.
pub fn ensure_sqlite_stat1_table(
    pager: &mut Pager,
    header: &DatabaseHeader,
) -> Result<u32, BtreeError> {
    if let Some(existing) = find_master_rootpage(pager, header, "sqlite_stat1")? {
        return Ok(existing);
    }

    let root_page = pager.allocate_page()?;
    let header_start = page1_header_start(root_page);
    let buf = pager.get_page_mut(root_page)?;
    write_leaf_page(buf, header_start, root_page, &[])?;

    insert_master_row(
        pager,
        header,
        &MasterEntry {
            kind: "table".to_string(),
            name: "sqlite_stat1".to_string(),
            tbl_name: "sqlite_stat1".to_string(),
            rootpage: root_page,
            sql: SQLITE_STAT1_SQL.to_string(),
        },
    )?;
    bump_schema_cookie(pager)?;

    Ok(root_page)
}

/// Deletes every `sqlite_stat1` row whose `tbl` column equals
/// `table_name` — `ANALYZE table-name` (#461) calls this before
/// re-inserting fresh rows, so a re-run replaces rather than appends to
/// that table's stats (spec 011/Req 2).
pub fn delete_stat1_rows_for_table(
    pager: &mut Pager,
    header: &DatabaseHeader,
    stat1_root: u32,
    table_name: &str,
) -> Result<(), BtreeError> {
    let mut cursor = crate::btree::TableCursor::new(&*pager, header, stat1_root);
    let mut stale_rowids = Vec::new();
    let mut row = cursor.first_row()?;
    while let Some(r) = row {
        let values = decode_record(&r.payload, header.text_encoding)?;
        if let Some(Value::Text(tbl)) = values.first() {
            if tbl.as_ref() == table_name {
                stale_rowids.push(r.rowid);
            }
        }
        row = cursor.next_row()?;
    }
    for rowid in stale_rowids {
        super::delete_row(pager, header, stat1_root, rowid)?;
    }
    Ok(())
}

/// Inserts one `sqlite_stat1` row: `idx = None` for a table's own
/// row-count row, `idx = Some(name)` for one of its indexes.
pub fn insert_stat1_row(
    pager: &mut Pager,
    header: &DatabaseHeader,
    stat1_root: u32,
    tbl: &str,
    idx: Option<&str>,
    stat: &str,
) -> Result<(), BtreeError> {
    let next_rowid = max_rowid(pager, header, stat1_root)?
        .unwrap_or(0)
        .saturating_add(1);
    let idx_value = match idx {
        Some(name) => Value::Text(name.into()),
        None => Value::Null,
    };
    let values = [Value::Text(tbl.into()), idx_value, Value::Text(stat.into())];
    let payload = encode_record(&values, header.text_encoding);
    super::insert_row(pager, header, stat1_root, next_rowid, &payload)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing, clippy::expect_used)]
mod tests {
    use super::*;
    use std::path::Path;

    use super::super::test_minimal_db as minimal_db;

    #[test]
    fn find_master_rootpage_rejects_out_of_range_rootpage() {
        let page_size = 512u32;
        let (vfs, header) = minimal_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();

        // Craft a sqlite_master row with a negative rootpage — something a
        // stock MasterEntry (rootpage: u32) can never produce, but a
        // corrupted/malicious .db file can.
        let values = [
            Value::Text("table".to_string().into()),
            Value::Text("evil".to_string().into()),
            Value::Text("evil".to_string().into()),
            Value::Integer(-1),
            Value::Text(String::new().into()),
        ];
        let payload = encode_record(&values, header.text_encoding);
        super::super::insert_row(&mut pager, &header, SQLITE_MASTER_ROOT_PAGE, 1, &payload)
            .unwrap();

        let err = find_master_rootpage(&mut pager, &header, "evil").unwrap_err();
        assert!(matches!(
            err,
            BtreeError::InvalidRootPage { rootpage: -1, .. }
        ));
    }

    #[test]
    fn schema_cookie_increments_and_persists() {
        let page_size = 512u32;
        let (vfs, header) = minimal_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();

        assert_eq!(header.schema_cookie, 0);
        let new_cookie = bump_schema_cookie(&mut pager).unwrap();
        assert_eq!(new_cookie, 1);
        pager.flush().unwrap();

        let mut pager2 = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();
        assert_eq!(read_schema_cookie(&mut pager2).unwrap(), 1);
    }

    #[test]
    fn insert_then_delete_master_row_round_trips() {
        let page_size = 512u32;
        let (vfs, header) = minimal_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();

        insert_master_row(
            &mut pager,
            &header,
            &MasterEntry {
                kind: "table".to_string(),
                name: "t".to_string(),
                tbl_name: "t".to_string(),
                rootpage: 2,
                sql: "CREATE TABLE t(a INTEGER, b TEXT)".to_string(),
            },
        )
        .unwrap();

        let mut cursor = crate::btree::TableCursor::new(&pager, &header, SQLITE_MASTER_ROOT_PAGE);
        let row = cursor.first_row().unwrap().unwrap();
        let values = decode_record(&row.payload, header.text_encoding).unwrap();
        assert_eq!(values[1], Value::Text("t".to_string().into()));
        assert_eq!(values[3], Value::Integer(2));

        delete_master_row(&mut pager, &header, "t").unwrap();
        let mut cursor2 = crate::btree::TableCursor::new(&pager, &header, SQLITE_MASTER_ROOT_PAGE);
        assert!(cursor2.first_row().unwrap().is_none());
    }

    #[test]
    fn delete_master_row_missing_name_errors() {
        let page_size = 512u32;
        let (vfs, header) = minimal_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();

        let err = delete_master_row(&mut pager, &header, "nope").unwrap_err();
        assert!(matches!(err, BtreeError::MasterEntryNotFound { .. }));
    }

    #[test]
    fn update_sequence_creates_table_on_first_use() {
        let page_size = 512u32;
        let (vfs, header) = minimal_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();

        update_sequence(&mut pager, &header, "t", 5).unwrap();

        let seq_root = find_master_rootpage(&mut pager, &header, "sqlite_sequence")
            .unwrap()
            .expect("sqlite_sequence registered in sqlite_master");
        let mut cursor = crate::btree::TableCursor::new(&pager, &header, seq_root);
        let row = cursor.first_row().unwrap().unwrap();
        let values = decode_record(&row.payload, header.text_encoding).unwrap();
        assert_eq!(values[0], Value::Text("t".to_string().into()));
        assert_eq!(values[1], Value::Integer(5));
    }

    #[test]
    fn update_sequence_only_grows() {
        let page_size = 512u32;
        let (vfs, header) = minimal_db(page_size);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();

        update_sequence(&mut pager, &header, "t", 5).unwrap();
        update_sequence(&mut pager, &header, "t", 3).unwrap();

        let seq_root = find_master_rootpage(&mut pager, &header, "sqlite_sequence")
            .unwrap()
            .unwrap();
        let mut cursor = crate::btree::TableCursor::new(&pager, &header, seq_root);
        let row = cursor.first_row().unwrap().unwrap();
        let values = decode_record(&row.payload, header.text_encoding).unwrap();
        assert_eq!(values[1], Value::Integer(5));

        update_sequence(&mut pager, &header, "t", 9).unwrap();
        let mut cursor2 = crate::btree::TableCursor::new(&pager, &header, seq_root);
        let row2 = cursor2.first_row().unwrap().unwrap();
        let values2 = decode_record(&row2.payload, header.text_encoding).unwrap();
        assert_eq!(values2[1], Value::Integer(9));
    }
}
