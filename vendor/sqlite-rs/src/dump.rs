// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! Whole-database read: schema + every row of every table, the shared
//! core behind the `dump` and `export` CLI subcommands (issue #37, V1
//! step 9 — the acceptance gate for epic #5).
//!
//! Opening a database or reading its schema is a hard failure (the file
//! can't safely be read at all). A single table failing to decode is a
//! "graceful unknown" (spec 001-architecture Requirement 4): it's
//! skipped, a warning is recorded, and every other table still comes
//! out. Virtual tables (no b-tree storage of their own) are always
//! skipped with a warning, never attempted.

use std::path::Path;

use crate::btree::{BtreeError, IndexCursor, IndexRow, TableCursor, TableRow};
use crate::header::{DatabaseHeader, HeaderError, HEADER_LEN};
use crate::pager::{Pager, PagerError};
use crate::record::{decode_record, RecordError, TextEncoding, Value};
use crate::schema::{column_defs, column_type, read_schema, DdlError, TableSchema};
use crate::vfs::{PageSource, Vfs, VfsError};

/// Failure reading a database while producing a [`DumpResult`].
#[derive(Debug)]
pub enum DumpError {
    /// Failure opening or reading the database file through the VFS.
    Vfs(VfsError),

    /// The 100-byte database header failed to parse.
    Header(HeaderError),

    /// Failure opening the pager over the database file.
    Pager(PagerError),

    /// Failure reading `sqlite_master` into a schema.
    Schema(DdlError),
}

impl std::fmt::Display for DumpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DumpError::Vfs(e) => write!(f, "{e}"),
            DumpError::Header(e) => write!(f, "parsing database header: {e}"),
            DumpError::Pager(e) => write!(f, "opening pager: {e}"),
            DumpError::Schema(e) => write!(f, "reading schema: {e}"),
        }
    }
}

impl std::error::Error for DumpError {}

impl From<VfsError> for DumpError {
    fn from(e: VfsError) -> Self {
        DumpError::Vfs(e)
    }
}

impl From<HeaderError> for DumpError {
    fn from(e: HeaderError) -> Self {
        DumpError::Header(e)
    }
}

impl From<PagerError> for DumpError {
    fn from(e: PagerError) -> Self {
        DumpError::Pager(e)
    }
}

impl From<DdlError> for DumpError {
    fn from(e: DdlError) -> Self {
        DumpError::Schema(e)
    }
}

/// One table's decoded rows, ready for `-list`/`-csv` rendering. The
/// rowid-alias column (a lone `INTEGER PRIMARY KEY` column, whose value
/// is never actually stored in the record — see spike 003 finding 1)
/// has already been substituted with the row's real rowid.
pub struct TableDump {
    /// The table's name.
    pub name: String,
    /// The verbatim `CREATE TABLE` statement from `sqlite_master`.
    pub sql: String,
    /// Column names, in declared order.
    pub columns: Vec<String>,
    /// Decoded rows, each position-for-position with `columns`.
    pub rows: Vec<Vec<Value>>,
}

/// Every readable table in a database, plus warnings for anything
/// skipped. `tables` preserves `sqlite_master` order.
pub struct DumpResult {
    /// Every successfully-read table, in `sqlite_master` order.
    pub tables: Vec<TableDump>,
    /// One message per table skipped or that failed to decode.
    pub warnings: Vec<String>,
}

/// Bootstraps just `page_size` from raw page-1 bytes (magic string plus
/// the page-size field, bytes 16-17 with the `1` = 65536 encoding),
/// without the rest of [`DatabaseHeader::parse`]'s validation — used by
/// [`open`]'s fallback path (#390), where fields set only by a
/// transaction that hasn't reached the main file yet (see that
/// function's doc comment) would otherwise make a full parse fail
/// before `page_size` is ever in hand.
fn bootstrap_page_size(raw: &[u8]) -> Option<u32> {
    if raw.get(0..16)? != b"SQLite format 3\0" {
        return None;
    }
    let raw_page_size = u16::from_be_bytes([*raw.get(16)?, *raw.get(17)?]);
    Some(if raw_page_size == 1 {
        65536
    } else {
        raw_page_size as u32
    })
}

/// Opens `path` through `vfs`, parses its header, and returns a
/// [`Pager`] over it. Shared by both `dump_database` and any caller that
/// needs the same open path (e.g. re-opening per table, since [`Pager`]
/// isn't [`Clone`] and this crate's own tests establish "open fresh per
/// cursor" as the pattern — see `src/pager/mod.rs`'s fixture tests).
///
/// The common case parses the header straight from the main file's raw
/// bytes, same as always. But for a WAL-mode database whose very first
/// schema-creating transaction hasn't been checkpointed yet (#390's live
/// interop tests hit this: a real, live second connection can leave a
/// database in exactly this state), the *main* file's page 1 only has
/// `page_size` and the journal-mode format bytes set — written directly
/// to disk immediately on the mode switch, mirroring
/// `Pager::set_journal_mode`'s own "never through `flush`" header-byte
/// flip — while everything else (`text_encoding` included) is still
/// zeroed: that transaction's real, valid page 1 exists only in the
/// `-wal` file's frames until a checkpoint backfills it. When the raw
/// parse fails, this falls back to bootstrapping just `page_size`
/// leniently, opening the `Pager` (whose `read_page` *does* merge WAL
/// frames — spec 007 Requirement 3), and re-deriving the real header
/// from that WAL-aware read instead of giving up.
pub fn open<V: Vfs + Clone + 'static>(
    vfs: &V,
    path: &Path,
) -> Result<(DatabaseHeader, Pager), DumpError> {
    let file = vfs.open_read(path)?;
    let mut header_buf = [0u8; HEADER_LEN];
    file.read_at(&mut header_buf, 0)?;
    match DatabaseHeader::parse(&header_buf) {
        Ok(header) => {
            let pager = Pager::open(vfs, path, header.page_size)?;
            Ok((header, pager))
        }
        Err(e) => {
            let page_size = bootstrap_page_size(&header_buf).ok_or(e)?;
            let pager = Pager::open(vfs, path, page_size)?;
            let page1 = pager.read_page(1).map_err(PagerError::from)?;
            let mut buf = [0u8; HEADER_LEN];
            let head = page1
                .get(..HEADER_LEN)
                .ok_or(HeaderError::TooShort { len: page1.len() })?;
            buf.copy_from_slice(head);
            let header = DatabaseHeader::parse(&buf)?;
            Ok((header, pager))
        }
    }
}

/// Reads every table's schema and rows out of the database at `path`.
pub fn dump_database<V: Vfs + Clone + 'static>(
    vfs: &V,
    path: &Path,
) -> Result<DumpResult, DumpError> {
    let (header, pager) = open(vfs, path)?;
    let mut schema_cursor = TableCursor::new(pager, &header, 1);
    let schemas = read_schema(&mut schema_cursor, header.text_encoding)?;

    let mut tables = Vec::new();
    let mut warnings = Vec::new();

    for schema in &schemas {
        if schema.is_virtual {
            warnings.push(format!(
                "table {:?}: virtual table, no storage of its own — skipped",
                schema.name
            ));
            continue;
        }
        let (_, pager) = match open(vfs, path) {
            Ok(v) => v,
            Err(e) => {
                warnings.push(format!("table {:?}: reopening database: {e}", schema.name));
                continue;
            }
        };
        match read_table_rows(pager, &header, schema) {
            Ok(rows) => tables.push(TableDump {
                name: schema.name.clone(),
                sql: schema.sql.clone(),
                columns: schema.columns.clone(),
                rows,
            }),
            Err(e) => warnings.push(format!("table {:?}: {e}", schema.name)),
        }
    }

    Ok(DumpResult { tables, warnings })
}

/// Column indices with REAL type affinity (declared type containing
/// `REAL`, `FLOA`, or `DOUB`, per SQLite's type-affinity rules). Needed
/// because SQLite stores any REAL value with no fractional component
/// (not just `0.0`/`1.0`, which additionally get the dedicated
/// serial-type-8/9 encoding) using an integer serial type in a
/// REAL-affinity column, to save space — the raw record alone can't
/// tell that apart from a genuine INTEGER without the column's declared
/// type. Confirmed empirically against `sqlite3 -list` (a REAL column
/// holding `42.0` renders `42.0`, not `42`).
fn real_affinity_columns(schema: &TableSchema) -> Vec<bool> {
    column_defs(schema)
        .iter()
        .map(|def| {
            let declared_type = column_type(def).to_ascii_uppercase();
            declared_type.contains("REAL")
                || declared_type.contains("FLOA")
                || declared_type.contains("DOUB")
        })
        .collect()
}

fn apply_real_affinity(values: &mut [Value], real_affinity: &[bool]) {
    for (i, is_real) in real_affinity.iter().enumerate() {
        if !is_real {
            continue;
        }
        if let Some(v) = values.get_mut(i) {
            if let Value::Integer(n) = v {
                *v = Value::Real(*n as f64);
            }
        }
    }
}

fn read_table_rows<P: PageSource>(
    source: P,
    header: &DatabaseHeader,
    schema: &TableSchema,
) -> Result<Vec<Vec<Value>>, TableReadError> {
    let alias_col = schema.rowid_alias;
    let real_affinity = real_affinity_columns(schema);
    if schema.without_rowid {
        let mut cursor = IndexCursor::new(source, header.usable_page_size(), schema.root_page);
        let mut rows = Vec::new();
        let mut row = cursor.first()?;
        while let Some(r) = row {
            let mut values = decode_index_row(&r, header.text_encoding)?;
            apply_real_affinity(&mut values, &real_affinity);
            rows.push(values);
            row = cursor.next()?;
        }
        Ok(rows)
    } else {
        let mut cursor = TableCursor::new(source, header, schema.root_page);
        let mut rows = Vec::new();
        let mut row = cursor.first_row()?;
        while let Some(r) = row {
            let mut values = decode_table_row(&r, header.text_encoding, alias_col)?;
            apply_real_affinity(&mut values, &real_affinity);
            rows.push(values);
            row = cursor.next_row()?;
        }
        Ok(rows)
    }
}

fn decode_table_row(
    row: &TableRow,
    encoding: TextEncoding,
    alias_col: Option<usize>,
) -> Result<Vec<Value>, TableReadError> {
    let mut values = decode_record(&row.payload, encoding)?;
    if let Some(idx) = alias_col {
        if let Some(v) = values.get_mut(idx) {
            *v = Value::Integer(row.rowid);
        }
    }
    Ok(values)
}

fn decode_index_row(row: &IndexRow, encoding: TextEncoding) -> Result<Vec<Value>, TableReadError> {
    Ok(decode_record(&row.payload, encoding)?)
}

#[derive(Debug)]
enum TableReadError {
    Btree(BtreeError),
    Record(RecordError),
}

impl std::fmt::Display for TableReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TableReadError::Btree(e) => write!(f, "{e}"),
            TableReadError::Record(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for TableReadError {}

impl From<BtreeError> for TableReadError {
    fn from(e: BtreeError) -> Self {
        TableReadError::Btree(e)
    }
}

impl From<RecordError> for TableReadError {
    fn from(e: RecordError) -> Self {
        TableReadError::Record(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema(sql: &str) -> TableSchema {
        TableSchema {
            unresolved_autoindex: false,
            name: "t".to_string(),
            root_page: 1,
            columns: vec![],
            column_types: vec![],
            column_collations: vec![],
            without_rowid: false,
            strict: false,
            is_virtual: false,
            sql: sql.to_string(),
            indexes: vec![],
            rowid_alias: None,
        }
        .with_computed_rowid_alias()
    }

    #[test]
    fn rowid_alias_detects_plain_integer_primary_key() {
        let s = schema("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
        assert_eq!(s.rowid_alias, Some(0));
    }

    #[test]
    fn rowid_alias_none_for_without_rowid() {
        let mut s = schema("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
        s.without_rowid = true;
        // `rowid_alias` is resolved at construction (#589), so a caller
        // mutating `without_rowid` afterwards must recompute it.
        let s = s.with_computed_rowid_alias();
        assert_eq!(s.rowid_alias, None);
    }

    #[test]
    fn real_affinity_detects_declared_real_types() {
        let s = schema("CREATE TABLE t (a REAL, b DOUBLE, c FLOAT, d INTEGER)");
        assert_eq!(real_affinity_columns(&s), vec![true, true, true, false]);
    }

    // Regression cases for `column_defs`/`rowid_alias_from_sql`/
    // `real_affinity_columns` re-deriving column info from raw DDL text:
    // quote/comment-aware splitting (#135) and declared-type-token
    // matching (#181) keep these from false-positiving on constraint or
    // string-literal text — see PR #49 review discussion.

    #[test]
    fn comma_inside_string_literal_default_does_not_missplit_columns() {
        // `DEFAULT 'a,b'` contains a comma that isn't a column separator;
        // `column_defs` is now quote-aware (#135) and doesn't split on it.
        let s = schema("CREATE TABLE t (a TEXT DEFAULT 'a,b', b INTEGER)");
        assert_eq!(
            column_defs(&s).len(),
            2,
            "comma inside a string literal must not be treated as a column separator"
        );
    }

    #[test]
    fn constraint_text_mentioning_affinity_keywords_is_not_a_false_positive() {
        // A CHECK constraint mentioning "FLOAT" as a string, not a type,
        // must not make `real_affinity_columns` treat the column as
        // REAL — it matches only the declared-type token (via
        // `column_type`), not the whole column-definition remainder.
        let s = schema("CREATE TABLE t (a TEXT CHECK(a != 'FLOAT'))");
        assert_eq!(
            real_affinity_columns(&s),
            vec![false],
            "constraint text mentioning FLOAT must not be detected as REAL affinity"
        );
    }

    // The two `rowid_alias_from_sql` cases below used to be the reason
    // that function's naivety carried more weight than it did before
    // #135: since #96, `src/codegen/expr.rs` emits `Rowid` instead of
    // `Column` based on its answer, so a wrong index is a wrong query
    // result rather than only wrong `dump` output.

    #[test]
    fn string_literal_mentioning_primary_key_is_not_a_false_positive() {
        // The DEFAULT literal contains the PRIMARY/KEY token pair, but
        // #135's quote-aware scan masks string-literal content before
        // looking for the constraint, so it no longer reads as one.
        let s = schema("CREATE TABLE t (a INTEGER DEFAULT 'primary key', b TEXT)");
        assert_eq!(
            s.rowid_alias, None,
            "PRIMARY KEY inside a string literal must not be read as a real constraint"
        );
    }

    #[test]
    fn table_level_primary_key_is_detected_as_the_alias() {
        // SQLite treats `CREATE TABLE t(x INTEGER, PRIMARY KEY(x))` as a
        // rowid alias; #135 makes `rowid_alias_from_sql` check the
        // table-level constraint form, not just an inline one.
        let s = schema("CREATE TABLE t (x INTEGER, PRIMARY KEY(x))");
        assert_eq!(
            s.rowid_alias,
            Some(0),
            "table-level PRIMARY KEY(x) over an INTEGER column is a rowid alias in SQLite"
        );
    }

    #[test]
    fn table_level_primary_key_over_two_columns_is_not_the_alias() {
        // A composite table-level PRIMARY KEY is never a rowid alias,
        // even though every named column is INTEGER — SQLite only
        // grants the optimization to a single-column key.
        let s = schema("CREATE TABLE t (x INTEGER, y INTEGER, PRIMARY KEY(x, y))");
        assert_eq!(s.rowid_alias, None);
    }

    #[test]
    fn escaped_quote_inside_string_literal_does_not_end_it_early() {
        // `'it''s'` is a single string literal containing a literal
        // apostrophe (SQL's doubled-quote escape) — the comma that
        // follows must still be seen as a column separator, and the
        // constraint text after it must not be split mid-literal.
        let s = schema("CREATE TABLE t (a TEXT DEFAULT 'it''s, tricky', b INTEGER)");
        assert_eq!(column_defs(&s).len(), 2);
    }

    #[test]
    fn comment_containing_a_comma_does_not_split_columns() {
        let s = schema("CREATE TABLE t (a INTEGER /* a, b */, b TEXT)");
        assert_eq!(column_defs(&s).len(), 2);
    }

    #[test]
    fn line_comment_containing_primary_key_does_not_false_positive() {
        let s = schema("CREATE TABLE t (a INTEGER -- not primary key\n, b TEXT)");
        assert_eq!(s.rowid_alias, None);
    }

    #[test]
    fn bracket_quoted_identifier_with_comma_does_not_split_columns() {
        let s = schema("CREATE TABLE t ([a,b] INTEGER, c TEXT)");
        assert_eq!(column_defs(&s).len(), 2);
    }

    #[test]
    fn rowid_alias_none_for_integer_primary_key_desc() {
        // Not fragile — a real rule: the DESC form gets its own b-tree
        // index and stores the column normally, so it must NOT be
        // substituted (SQLite's "ROWIDs and the INTEGER PRIMARY KEY").
        let s = schema("CREATE TABLE t (id INTEGER PRIMARY KEY DESC, name TEXT)");
        assert_eq!(s.rowid_alias, None);
    }
}
