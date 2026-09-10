// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! `PRAGMA integrity_check`/`quick_check` (#540, #541): walks every table
//! and index b-tree plus the freelist chain, reporting structural
//! problems in the same textual shape stock `sqlite3` uses -- a single
//! `"ok"` row when nothing is wrong, or one row per problem found.
//!
//! Scope: table-b-tree rowid ordering, index-b-tree key ordering, and
//! (for `integrity_check` only, not `quick_check`) the index-vs-table
//! cross-check -- every index entry's trailing rowid must exist in its
//! table, and every table row must be represented by every one of its
//! table's indexes. Also walks the freelist chain and checks its page
//! count against the header. Pointer-map cross-validation is out of
//! scope: this crate never writes a pointer-map (see `src/pager.rs`'s
//! module doc) since it has no auto-vacuum/incremental-vacuum support,
//! so a `largest_root_btree_page != 0` database (auto-vacuum, only ever
//! seen in externally-crafted fixtures) is reported as a single
//! informational problem rather than silently mis-validated.

use std::collections::HashSet;

use crate::btree::{IndexCursor, IndexRow, TableCursor};
use crate::header::DatabaseHeader;
use crate::pager::freelist::TrunkPage;
use crate::record::{decode_record, Value};
use crate::schema::{read_schema, IndexSchema, TableSchema};
use crate::vfs::PageSource;

/// Runs the check and returns `["ok"]` if nothing is wrong, or one
/// human-readable problem description per row otherwise. `quick` skips
/// the index-vs-table cross-check pass (`PRAGMA quick_check`, #541).
/// Generic over `P` (rather than a `dyn PageSource` trait object) to stay
/// inside the `mvl-limit` qualified subset (see `src/pager.rs`'s module
/// doc: only `src/vfs/` and the VDBE's `Rc<dyn PageSource>` boundary in
/// `src/vdbe/{exec,cursor}.rs` are exempt) -- the caller in
/// `src/vdbe/pragma.rs` passes its own `Rc<dyn PageSource>` as `P`.
pub fn run_integrity_check<P: PageSource + Clone>(
    source: P,
    header: &DatabaseHeader,
    quick: bool,
) -> Vec<String> {
    let mut problems = Vec::new();

    if header.largest_root_btree_page != 0 {
        problems.push(
            "auto-vacuum database: pointer-map cross-check is not implemented, skipped".to_string(),
        );
    }

    let mut master_cursor = TableCursor::new(source.clone(), header, 1);
    let schemas = match read_schema(&mut master_cursor, header.text_encoding) {
        Ok(s) => s,
        Err(e) => {
            problems.push(format!("*** in database main *** sqlite_master: {e}"));
            return problems;
        }
    };

    for table in &schemas {
        if table.is_virtual {
            continue;
        }
        let table_rowids = check_table(&source, header, table, &mut problems);
        if !quick {
            for index in &table.indexes {
                check_index(&source, header, table, index, &table_rowids, &mut problems);
            }
        }
    }

    check_freelist(&source, header, &mut problems);

    if problems.is_empty() {
        vec!["ok".to_string()]
    } else {
        problems
    }
}

/// Walks `table`'s b-tree via [`TableCursor`], checking rowids are
/// strictly increasing (the on-disk invariant every table b-tree must
/// satisfy). Returns the set of rowids seen, for the index cross-check.
fn check_table<P: PageSource + Clone>(
    source: &P,
    header: &DatabaseHeader,
    table: &TableSchema,
    problems: &mut Vec<String>,
) -> HashSet<i64> {
    let mut rowids = HashSet::new();
    let mut cursor = TableCursor::new(source.clone(), header, table.root_page);
    let mut prev: Option<i64> = None;
    let mut row = match cursor.first() {
        Ok(r) => r,
        Err(e) => {
            problems.push(format!(
                "*** in database main *** table {:?}: {e}",
                table.name
            ));
            return rowids;
        }
    };
    while let Some(rowid) = row {
        if let Some(p) = prev {
            if rowid <= p {
                problems.push(format!(
                    "*** in database main *** table {:?}: rowid {rowid} out of order after {p}",
                    table.name
                ));
            }
        }
        if !rowids.insert(rowid) {
            problems.push(format!(
                "*** in database main *** table {:?}: duplicate rowid {rowid}",
                table.name
            ));
        }
        prev = Some(rowid);
        row = match cursor.next() {
            Ok(r) => r,
            Err(e) => {
                problems.push(format!(
                    "*** in database main *** table {:?}: {e}",
                    table.name
                ));
                None
            }
        };
    }
    rowids
}

/// Walks `index`'s b-tree via [`IndexCursor`], checking key ordering and
/// (the "exhaustive" part `quick_check` skips) cross-checking every
/// decoded entry's trailing rowid against `table_rowids`, plus that the
/// index has exactly as many entries as the table has rows.
fn check_index<P: PageSource + Clone>(
    source: &P,
    header: &DatabaseHeader,
    table: &TableSchema,
    index: &IndexSchema,
    table_rowids: &HashSet<i64>,
    problems: &mut Vec<String>,
) {
    let mut cursor = IndexCursor::new(source.clone(), header.usable_page_size(), index.root_page);
    let mut prev_key: Option<Vec<Value>> = None;
    let mut seen = 0usize;
    let mut row = match cursor.first() {
        Ok(r) => r,
        Err(e) => {
            problems.push(format!(
                "*** in database main *** index {:?}: {e}",
                index.name
            ));
            return;
        }
    };
    while let Some(entry) = row {
        let decoded = match decode_record(&entry.payload, header.text_encoding) {
            Ok(v) => v,
            Err(e) => {
                problems.push(format!(
                    "*** in database main *** index {:?}: malformed entry: {e}",
                    index.name
                ));
                row = advance(&mut cursor, index, problems);
                continue;
            }
        };
        let Some(Value::Integer(rowid)) = decoded.last() else {
            problems.push(format!(
                "*** in database main *** index {:?}: entry has no trailing rowid",
                index.name
            ));
            row = advance(&mut cursor, index, problems);
            continue;
        };
        if !table_rowids.contains(rowid) {
            problems.push(format!(
                "*** in database main *** index {:?}: entry references rowid {rowid} not present in table {:?}",
                index.name, table.name
            ));
        }
        let key = decoded
            .get(..decoded.len().saturating_sub(1))
            .unwrap_or(&[]);
        if let Some(p) = &prev_key {
            if compare_index_keys(p.as_slice(), key) == std::cmp::Ordering::Greater {
                problems.push(format!(
                    "*** in database main *** index {:?}: keys out of order",
                    index.name
                ));
            }
        }
        prev_key = Some(key.to_vec());
        seen = seen.saturating_add(1);
        row = advance(&mut cursor, index, problems);
    }
    if seen != table_rowids.len() {
        problems.push(format!(
            "*** in database main *** wrong # of entries in index {:?}: expected {}, found {seen}",
            index.name,
            table_rowids.len()
        ));
    }
}

fn advance<P: PageSource>(
    cursor: &mut IndexCursor<P>,
    index: &IndexSchema,
    problems: &mut Vec<String>,
) -> Option<IndexRow> {
    match cursor.next() {
        Ok(r) => r,
        Err(e) => {
            problems.push(format!(
                "*** in database main *** index {:?}: {e}",
                index.name
            ));
            None
        }
    }
}

/// Lexicographic comparison over a decoded (possibly composite) index
/// key, `BINARY`-collation only -- per-column `COLLATE` is not applied
/// here (known limitation; matches this checker's scope, not stock
/// SQLite's full collation-aware `integrity_check`).
fn compare_index_keys(a: &[Value], b: &[Value]) -> std::cmp::Ordering {
    for (x, y) in a.iter().zip(b.iter()) {
        let c = compare_values(x, y);
        if c != std::cmp::Ordering::Equal {
            return c;
        }
    }
    a.len().cmp(&b.len())
}

fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
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
        _ => Ordering::Equal,
    }
}

/// Walks the freelist trunk chain from `header.freelist_trunk_page`,
/// checking that the number of leaf pages visited matches
/// `header.freelist_page_count` and that no page number repeats or is
/// out of range.
fn check_freelist<P: PageSource>(source: &P, header: &DatabaseHeader, problems: &mut Vec<String>) {
    if header.freelist_trunk_page == 0 {
        if header.freelist_page_count != 0 {
            problems.push(format!(
                "freelist_page_count is {} but there is no freelist trunk page",
                header.freelist_page_count
            ));
        }
        return;
    }
    let mut seen_trunks = HashSet::new();
    let mut total_leaves = 0u32;
    let mut trunk = header.freelist_trunk_page;
    let max_hops = header.page_count.saturating_add(1);
    let mut hops = 0u32;
    while trunk != 0 {
        hops = hops.saturating_add(1);
        if hops > max_hops {
            problems.push(
                "freelist trunk chain longer than the database's page count (cycle?)".to_string(),
            );
            break;
        }
        if trunk > header.page_count || !seen_trunks.insert(trunk) {
            problems.push(format!(
                "freelist trunk page {trunk} is out of range or repeated"
            ));
            break;
        }
        let buf = match source.read_page(trunk) {
            Ok(b) => b,
            Err(e) => {
                problems.push(format!("reading freelist trunk page {trunk}: {e}"));
                break;
            }
        };
        let page = match TrunkPage::parse(&buf) {
            Ok(p) => p,
            Err(e) => {
                problems.push(format!("parsing freelist trunk page {trunk}: {e}"));
                break;
            }
        };
        for leaf in &page.leaves {
            if *leaf == 0 || *leaf > header.page_count {
                problems.push(format!("freelist leaf page {leaf} is out of range"));
            }
        }
        total_leaves = total_leaves.saturating_add(page.leaves.len() as u32);
        trunk = page.next_trunk;
    }
    let total = total_leaves.saturating_add(seen_trunks.len() as u32);
    if total != header.freelist_page_count {
        problems.push(format!(
            "freelist_page_count is {} but the trunk chain has {total} pages",
            header.freelist_page_count
        ));
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::arithmetic_side_effects
)]
mod tests {
    use std::collections::HashMap;
    use std::rc::Rc;

    use super::*;
    use crate::record::TextEncoding;
    use crate::schema::IndexedColumn;
    use crate::vfs::{PageError, PageSource};

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

    impl Clone for FakePageSource {
        fn clone(&self) -> Self {
            FakePageSource {
                pages: self.pages.clone(),
            }
        }
    }

    fn fake_header(page_count: u32) -> DatabaseHeader {
        DatabaseHeader {
            page_size: 512,
            write_version: 1,
            read_version: 1,
            reserved_space: 0,
            page_count,
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

    fn table_schema(name: &str, root_page: u32, indexes: Vec<IndexSchema>) -> TableSchema {
        TableSchema {
            unresolved_autoindex: false,
            name: name.to_string(),
            root_page,
            columns: vec![],
            column_types: vec![],
            column_collations: vec![],
            without_rowid: false,
            strict: false,
            is_virtual: false,
            sql: String::new(),
            indexes,
            rowid_alias: None,
        }
    }

    fn index_schema(name: &str, root_page: u32) -> IndexSchema {
        IndexSchema {
            name: name.to_string(),
            unique: false,
            columns: vec![IndexedColumn {
                name: "a".to_string(),
                desc: false,
                collation: crate::vdbe::Collation::Binary,
            }],
            root_page,
        }
    }

    // ---- compare_values / compare_index_keys ----

    #[test]
    fn compare_values_covers_every_type_pairing() {
        use std::cmp::Ordering;
        assert_eq!(compare_values(&Value::Null, &Value::Null), Ordering::Equal);
        assert_eq!(
            compare_values(&Value::Null, &Value::Integer(1)),
            Ordering::Less
        );
        assert_eq!(
            compare_values(&Value::Integer(1), &Value::Null),
            Ordering::Greater
        );
        assert_eq!(
            compare_values(&Value::Integer(1), &Value::Integer(2)),
            Ordering::Less
        );
        assert_eq!(
            compare_values(&Value::Real(1.0), &Value::Real(2.0)),
            Ordering::Less
        );
        assert_eq!(
            compare_values(&Value::Real(f64::NAN), &Value::Real(1.0)),
            Ordering::Equal
        );
        assert_eq!(
            compare_values(&Value::Integer(1), &Value::Real(2.0)),
            Ordering::Less
        );
        assert_eq!(
            compare_values(&Value::Real(2.0), &Value::Integer(1)),
            Ordering::Greater
        );
        assert_eq!(
            compare_values(&Value::Text("a".into()), &Value::Text("b".into())),
            Ordering::Less
        );
        assert_eq!(
            compare_values(
                &Value::Blob(vec![1u8].into()),
                &Value::Blob(vec![2u8].into())
            ),
            Ordering::Less
        );
        // Mismatched, non-NULL types fall through to the catch-all arm.
        assert_eq!(
            compare_values(&Value::Integer(1), &Value::Text("x".into())),
            Ordering::Equal
        );
    }

    #[test]
    fn compare_index_keys_breaks_ties_by_length() {
        use std::cmp::Ordering;
        let a = vec![Value::Integer(1)];
        let b = vec![Value::Integer(1), Value::Integer(2)];
        assert_eq!(compare_index_keys(&a, &b), Ordering::Less);
        assert_eq!(compare_index_keys(&b, &a), Ordering::Greater);
        assert_eq!(compare_index_keys(&a, &a), Ordering::Equal);
    }

    // ---- table b-tree page building ----

    fn leaf_table_page(rows: &[i64]) -> Vec<u8> {
        let page_size = 512usize;
        let mut page = vec![0u8; page_size];
        page[0] = 0x0d;
        page[3..5].copy_from_slice(&(rows.len() as u16).to_be_bytes());
        let ptr_base = 8usize;
        let mut cursor = ptr_base + rows.len() * 2;
        for (i, &rowid) in rows.iter().enumerate() {
            let cell = vec![0u8, rowid as u8]; // payload_len=0, rowid (small, positive)
            let start = cursor;
            page[start..start + cell.len()].copy_from_slice(&cell);
            page[ptr_base + i * 2..ptr_base + i * 2 + 2]
                .copy_from_slice(&(start as u16).to_be_bytes());
            cursor += cell.len();
        }
        page
    }

    #[test]
    fn check_table_reports_out_of_order_and_duplicate_rowids() {
        let page = leaf_table_page(&[5, 3, 3]);
        let mut pages = HashMap::new();
        pages.insert(7u32, page);
        let source = FakePageSource { pages };
        let header = fake_header(10);
        let table = table_schema("t", 7, vec![]);
        let mut problems = Vec::new();

        let rowids = check_table(&source, &header, &table, &mut problems);

        assert!(problems.iter().any(|p| p.contains("out of order")));
        assert!(problems.iter().any(|p| p.contains("duplicate rowid")));
        assert_eq!(rowids.len(), 2);
    }

    #[test]
    fn check_table_reports_cursor_error_on_missing_root_page() {
        let source = FakePageSource {
            pages: HashMap::new(),
        };
        let header = fake_header(10);
        let table = table_schema("missing", 99, vec![]);
        let mut problems = Vec::new();

        let rowids = check_table(&source, &header, &table, &mut problems);

        assert!(rowids.is_empty());
        assert_eq!(problems.len(), 1);
        assert!(problems[0].contains("missing"));
    }

    // ---- index b-tree page building ----

    fn encode_record_ints(values: &[i64]) -> Vec<u8> {
        let n = values.len();
        let header_size = 1 + n;
        let mut out = Vec::with_capacity(header_size + n);
        out.push(header_size as u8);
        out.extend(std::iter::repeat_n(1u8, n)); // serial type 1: 8-bit signed int
        for v in values {
            out.push(*v as i8 as u8);
        }
        out
    }

    fn leaf_index_page(rows: &[Vec<u8>]) -> Vec<u8> {
        let page_size = 512usize;
        let mut page = vec![0u8; page_size];
        page[0] = 0x0a;
        page[3..5].copy_from_slice(&(rows.len() as u16).to_be_bytes());
        let ptr_base = 8usize;
        let mut cursor = ptr_base + rows.len() * 2;
        for (i, payload) in rows.iter().enumerate() {
            let mut cell = vec![payload.len() as u8];
            cell.extend_from_slice(payload);
            let start = cursor;
            page[start..start + cell.len()].copy_from_slice(&cell);
            page[ptr_base + i * 2..ptr_base + i * 2 + 2]
                .copy_from_slice(&(start as u16).to_be_bytes());
            cursor += cell.len();
        }
        page
    }

    #[test]
    fn check_index_reports_missing_trailing_rowid_and_cross_check() {
        let entries = vec![
            encode_record_ints(&[9, 1]),
            vec![2u8, 0u8], // header_size=2, one NULL column -> no trailing rowid
        ];
        let page = leaf_index_page(&entries);
        let mut pages = HashMap::new();
        pages.insert(8u32, page);
        let source = FakePageSource { pages };
        let header = fake_header(10);
        let table = table_schema("t", 7, vec![]);
        let index = index_schema("t_a", 8);
        let mut table_rowids = HashSet::new();
        table_rowids.insert(2i64); // does not contain rowid 1, referenced above
        table_rowids.insert(3i64); // makes `seen` (1) mismatch table_rowids.len() (2)
        let mut problems = Vec::new();

        check_index(
            &source,
            &header,
            &table,
            &index,
            &table_rowids,
            &mut problems,
        );

        assert!(problems.iter().any(|p| p.contains("not present in table")));
        assert!(problems.iter().any(|p| p.contains("no trailing rowid")));
        assert!(problems.iter().any(|p| p.contains("wrong # of entries")));
    }

    #[test]
    fn check_index_reports_out_of_order_keys() {
        let entries = vec![encode_record_ints(&[5, 100]), encode_record_ints(&[3, 101])];
        let page = leaf_index_page(&entries);
        let mut pages = HashMap::new();
        pages.insert(8u32, page);
        let source = FakePageSource { pages };
        let header = fake_header(10);
        let table = table_schema("t", 7, vec![]);
        let index = index_schema("t_a", 8);
        let mut table_rowids = HashSet::new();
        table_rowids.insert(100i64);
        table_rowids.insert(101i64);
        let mut problems = Vec::new();

        check_index(
            &source,
            &header,
            &table,
            &index,
            &table_rowids,
            &mut problems,
        );

        assert!(problems.iter().any(|p| p.contains("keys out of order")));
    }

    #[test]
    fn check_index_reports_malformed_entry() {
        // header_len=5 but the payload is only 1 byte long, so the
        // header-walk itself runs off the end of the buffer.
        let entries = vec![vec![5u8]];
        let page = leaf_index_page(&entries);
        let mut pages = HashMap::new();
        pages.insert(8u32, page);
        let source = FakePageSource { pages };
        let header = fake_header(10);
        let table = table_schema("t", 7, vec![]);
        let index = index_schema("t_a", 8);
        let table_rowids = HashSet::new();
        let mut problems = Vec::new();

        check_index(
            &source,
            &header,
            &table,
            &index,
            &table_rowids,
            &mut problems,
        );

        assert!(problems.iter().any(|p| p.contains("malformed entry")));
    }

    #[test]
    fn check_index_reports_cursor_error_on_missing_root_page() {
        let source = FakePageSource {
            pages: HashMap::new(),
        };
        let header = fake_header(10);
        let table = table_schema("t", 7, vec![]);
        let index = index_schema("missing_idx", 99);
        let table_rowids = HashSet::new();
        let mut problems = Vec::new();

        check_index(
            &source,
            &header,
            &table,
            &index,
            &table_rowids,
            &mut problems,
        );

        assert_eq!(problems.len(), 1);
        assert!(problems[0].contains("missing_idx"));
    }

    // ---- run_integrity_check top-level ----

    #[test]
    fn run_integrity_check_flags_auto_vacuum_databases() {
        let mut header = fake_header(1);
        header.largest_root_btree_page = 3;
        // Page 1 must at least parse as an empty leaf table (sqlite_master).
        let mut page1 = vec![0u8; 512];
        page1[0] = 0x0d;
        let mut pages = HashMap::new();
        pages.insert(1u32, page1);
        let source = FakePageSource { pages };

        let problems = run_integrity_check(source, &header, false);

        assert!(problems.iter().any(|p| p.contains("auto-vacuum")));
    }

    #[test]
    fn run_integrity_check_reports_schema_read_failure() {
        let header = fake_header(1);
        let source = FakePageSource {
            pages: HashMap::new(),
        };

        let problems = run_integrity_check(source, &header, false);

        assert_eq!(problems.len(), 1);
        assert!(problems[0].contains("sqlite_master"));
    }

    // ---- freelist ----

    fn trunk_page(next_trunk: u32, leaves: &[u32]) -> Vec<u8> {
        let mut buf = vec![0u8; 512];
        buf[0..4].copy_from_slice(&next_trunk.to_be_bytes());
        buf[4..8].copy_from_slice(&(leaves.len() as u32).to_be_bytes());
        for (i, leaf) in leaves.iter().enumerate() {
            let off = 8 + i * 4;
            buf[off..off + 4].copy_from_slice(&leaf.to_be_bytes());
        }
        buf
    }

    #[test]
    fn check_freelist_reports_count_mismatch_when_no_trunk_page() {
        let mut header = fake_header(5);
        header.freelist_trunk_page = 0;
        header.freelist_page_count = 3;
        let source = FakePageSource {
            pages: HashMap::new(),
        };
        let mut problems = Vec::new();

        check_freelist(&source, &header, &mut problems);

        assert_eq!(problems.len(), 1);
        assert!(problems[0].contains("no freelist trunk page"));
    }

    #[test]
    fn check_freelist_is_silent_when_empty_and_consistent() {
        let header = fake_header(5);
        let source = FakePageSource {
            pages: HashMap::new(),
        };
        let mut problems = Vec::new();

        check_freelist(&source, &header, &mut problems);

        assert!(problems.is_empty());
    }

    #[test]
    fn check_freelist_reports_out_of_range_trunk() {
        let mut header = fake_header(2);
        header.freelist_trunk_page = 5; // > page_count
        header.freelist_page_count = 1;
        let source = FakePageSource {
            pages: HashMap::new(),
        };
        let mut problems = Vec::new();

        check_freelist(&source, &header, &mut problems);

        assert!(problems
            .iter()
            .any(|p| p.contains("out of range or repeated")));
    }

    #[test]
    fn check_freelist_reports_repeated_trunk_cycle() {
        let mut header = fake_header(2);
        header.freelist_trunk_page = 1;
        header.freelist_page_count = 99; // deliberately wrong, also exercises the mismatch line
        let mut pages = HashMap::new();
        pages.insert(1u32, trunk_page(2, &[]));
        pages.insert(2u32, trunk_page(1, &[]));
        let source = FakePageSource { pages };
        let mut problems = Vec::new();

        check_freelist(&source, &header, &mut problems);

        assert!(problems
            .iter()
            .any(|p| p.contains("out of range or repeated")));
        assert!(problems
            .iter()
            .any(|p| p.contains("but the trunk chain has")));
    }

    #[test]
    fn check_freelist_reports_read_error_on_missing_trunk_page() {
        let mut header = fake_header(5);
        header.freelist_trunk_page = 2;
        header.freelist_page_count = 1;
        let source = FakePageSource {
            pages: HashMap::new(),
        };
        let mut problems = Vec::new();

        check_freelist(&source, &header, &mut problems);

        assert!(problems
            .iter()
            .any(|p| p.contains("reading freelist trunk page")));
    }

    #[test]
    fn check_freelist_reports_parse_error_on_truncated_trunk_page() {
        let mut header = fake_header(5);
        header.freelist_trunk_page = 2;
        header.freelist_page_count = 1;
        let mut pages = HashMap::new();
        pages.insert(2u32, vec![0u8; 4]); // too short for the 8-byte trunk header
        let source = FakePageSource { pages };
        let mut problems = Vec::new();

        check_freelist(&source, &header, &mut problems);

        assert!(problems
            .iter()
            .any(|p| p.contains("parsing freelist trunk page")));
    }

    #[test]
    fn check_freelist_reports_out_of_range_leaf() {
        let mut header = fake_header(2);
        header.freelist_trunk_page = 1;
        header.freelist_page_count = 2;
        let mut pages = HashMap::new();
        pages.insert(1u32, trunk_page(0, &[999]));
        let source = FakePageSource { pages };
        let mut problems = Vec::new();

        check_freelist(&source, &header, &mut problems);

        assert!(problems
            .iter()
            .any(|p| p.contains("freelist leaf page 999 is out of range")));
    }
}
