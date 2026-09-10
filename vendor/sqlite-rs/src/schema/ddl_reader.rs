// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! `sqlite_master` decode + minimal DDL reader. Lives outside
//! `src/parser/` by design (spec 002-parser Requirement 5): zero
//! dependency on the full SQL parser, so schema decoding keeps working
//! with the parser feature-gated off. `src/parser/` now exists (and
//! already has a `CREATE INDEX` AST/parser), but this module deliberately
//! does not call into it — see #211.
//!
//! Unparseable DDL (e.g. a virtual table) degrades to raw-row access
//! (`columns: vec![]`), never an error — Tier 0's "graceful unknowns"
//! (spec 001-architecture Requirement 4). The same rule extends to
//! `CREATE INDEX` entries (#211): an index this reader can't find a
//! column list for (e.g. an auto-index, whose `sqlite_master.sql` is
//! `NULL`) is silently omitted from `TableSchema::indexes`.
//!
//! The DDL parser here is deliberately naive, matching spike 005 (#12)'s
//! prototype: no quoted/bracketed identifiers with embedded whitespace,
//! no dialect edge cases beyond what the real corpus fixtures exercise.
//! "Nothing more" than table name, column names, declared types (via
//! column name extraction only — types are not separately captured),
//! WITHOUT ROWID / STRICT markers, and (#211) each table's indexes
//! (column list, ASC/DESC, uniqueness), per the originating issues' scope.

use crate::btree::{BtreeError, TableCursor};
use crate::record::{decode_record, Collation, RecordError, TextEncoding, Value};
use crate::vfs::PageSource;

/// Failure walking or decoding `sqlite_master`.
#[derive(Debug)]
pub enum DdlError {
    /// Failure walking the `sqlite_master` b-tree.
    Btree(BtreeError),

    /// Failure decoding a `sqlite_master` row's record payload.
    Record(RecordError),

    /// A `sqlite_master` row didn't have exactly 5 columns.
    MalformedRow(usize),
}

impl std::fmt::Display for DdlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DdlError::Btree(e) => write!(f, "walking sqlite_master: {e}"),
            DdlError::Record(e) => write!(f, "decoding a sqlite_master row: {e}"),
            DdlError::MalformedRow(n) => {
                write!(f, "sqlite_master row has {n} columns, expected 5")
            }
        }
    }
}

impl std::error::Error for DdlError {}

impl From<BtreeError> for DdlError {
    fn from(e: BtreeError) -> Self {
        DdlError::Btree(e)
    }
}

impl From<RecordError> for DdlError {
    fn from(e: RecordError) -> Self {
        DdlError::Record(e)
    }
}

/// A minimally-parsed table schema entry: everything Tier 0 needs to
/// read a table without the full SQL parser.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSchema {
    /// The table's name.
    pub name: String,
    /// The table's b-tree root page (`sqlite_master.rootpage`).
    pub root_page: u32,
    /// Column names, in declared order.
    pub columns: Vec<String>,
    /// Whether the table was declared `WITHOUT ROWID`.
    pub without_rowid: bool,
    /// Whether the table was declared `STRICT`.
    pub strict: bool,
    /// Each column's declared type text, position-for-position with
    /// `columns` (empty string when a column has none) — the
    /// substring `affinity_of` (spec 008 Requirement 1) derives
    /// column affinity from. Comparison-affinity derivation (#138)
    /// needs this; `dump.rs` re-derives its own full column text from
    /// `sql` instead via [`column_defs`], so this is naive on purpose:
    /// same textual scope as `columns`, no dialect edge cases beyond
    /// what the corpus exercises.
    pub column_types: Vec<String>,
    /// Each column's declared `COLLATE` (default [`Collation::Binary`]
    /// when absent), position-for-position with `columns` — the
    /// crate-wide fallback #500 introduces for comparisons that don't
    /// spell out an explicit `COLLATE` in the query itself.
    pub column_collations: Vec<Collation>,
    /// `CREATE VIRTUAL TABLE ...` — DDL this reader deliberately does not
    /// parse. `columns` is always empty and `root_page` is `0` (virtual
    /// tables have no b-tree storage of their own).
    pub is_virtual: bool,
    /// The raw `sql` column text from `sqlite_master` — the verbatim
    /// `CREATE TABLE`/`CREATE VIRTUAL TABLE` statement, needed by callers
    /// that reproduce schema DDL verbatim (e.g. a `dump` CLI).
    pub sql: String,
    /// Every `CREATE INDEX`/`CREATE UNIQUE INDEX` entry in `sqlite_master`
    /// whose `tbl_name` is this table. Auto-indexes created implicitly for
    /// `PRIMARY KEY`/`UNIQUE` column constraints have a `NULL` `sql`
    /// column in `sqlite_master` and are not captured here — same
    /// graceful-degradation rule as unparseable DDL elsewhere in this
    /// reader (#211).
    pub indexes: Vec<IndexSchema>,
    /// Set when this table carries a `sqlite_autoindex_*` whose key
    /// columns could not be recovered from its own DDL (#685).
    ///
    /// Writes MUST be refused while this is set. The index exists on
    /// disk but is absent from `indexes`, so codegen would neither
    /// enforce its uniqueness nor maintain it — the write would succeed
    /// and leave the file corrupt. Same argument as spec 007/Req 1's
    /// hot-journal refusal: failing the operation beats silently
    /// producing a wrong database.
    pub unresolved_autoindex: bool,
    /// The rowid-alias column index (0-based into `columns`), resolved
    /// once at schema-decode time by [`rowid_alias_from_sql`] — SQLite's
    /// single-`INTEGER PRIMARY KEY` special case (see
    /// `src/btree/mod.rs`'s module doc). Codegen reads this field per
    /// column reference, so it must be a field, not a re-parse of `sql`
    /// on every call (#589).
    pub rowid_alias: Option<usize>,
}

impl TableSchema {
    /// Recomputes [`TableSchema::rowid_alias`] from `sql`/`without_rowid`
    /// — for callers (tests, synthetic schemas) that build a
    /// `TableSchema` literal by hand instead of going through
    /// [`read_schema`], which resolves the field at decode time (#589).
    #[must_use]
    pub fn with_computed_rowid_alias(mut self) -> Self {
        self.rowid_alias = if self.is_virtual {
            None
        } else {
            rowid_alias_from_sql(&self.sql, self.without_rowid)
        };
        self
    }
}

/// A minimally-parsed `CREATE INDEX` entry, naive in the same sense as
/// [`TableSchema`]: column list and ASC/DESC/uniqueness only, nothing more
/// (#211).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexSchema {
    /// The index's name.
    pub name: String,
    /// Whether the index was declared `UNIQUE`.
    pub unique: bool,
    /// The indexed columns, in declared key order.
    pub columns: Vec<IndexedColumn>,
    /// The index b-tree's root page (`sqlite_master.rootpage`), needed
    /// to `OpenWrite` a write cursor onto it (#196).
    pub root_page: u32,
}

/// One column (or expression, kept as raw text) in an index's key,
/// position-for-position with the index's declared column order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedColumn {
    /// The column's (unquoted) name, or raw expression text for an
    /// expression index.
    pub name: String,
    /// Whether this key part is sorted `DESC`.
    pub desc: bool,
    /// The key part's declared `COLLATE` (default [`Collation::Binary`]
    /// when absent).
    pub collation: Collation,
}

/// Walks `sqlite_master` via `cursor` (which callers MUST construct with
/// `root_page = 1`) and returns every `type = 'table'` entry as a
/// [`TableSchema`]. Never errors on unparseable or virtual-table DDL —
/// those degrade to `columns: vec![]`.
pub fn read_schema<P: PageSource>(
    cursor: &mut TableCursor<P>,
    encoding: TextEncoding,
) -> Result<Vec<TableSchema>, DdlError> {
    read_schema_and_views(cursor, encoding).map(|(schemas, _)| schemas)
}

/// Walks `sqlite_master` once and returns both the `type = 'table'`
/// entries (as [`read_schema`] would) and the `type = 'view'` entries
/// (as [`read_views`] would). Callers that need both catalogs —
/// `compile_statement` takes both — should use this instead of two
/// separate b-tree walks (#589).
pub fn read_schema_and_views<P: PageSource>(
    cursor: &mut TableCursor<P>,
    encoding: TextEncoding,
) -> Result<(Vec<TableSchema>, Vec<ViewSchema>), DdlError> {
    let mut schemas: Vec<TableSchema> = Vec::new();
    let mut views = Vec::new();
    // Table name → position in `schemas`, so index attachment below is
    // a hash lookup instead of an O(indexes × tables) linear scan (#589).
    let mut table_pos: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    let mut pending_indexes: Vec<(String, IndexSchema)> = Vec::new();
    // (table name, index name, root page) for `sqlite_autoindex_*` rows,
    // resolved after the walk because resolution needs the table's DDL.
    let mut pending_autoindexes: Vec<(String, String, u32)> = Vec::new();
    let mut row = cursor.first_row()?;
    while let Some(r) = row {
        let values = decode_record(&r.payload, encoding)?;
        if values.len() != 5 {
            return Err(DdlError::MalformedRow(values.len()));
        }
        match text(values.first()) {
            "table" => {
                let schema = table_schema(&values);
                table_pos.insert(schema.name.clone(), schemas.len());
                schemas.push(schema);
            }
            "index" => {
                if let Some(entry) = index_schema(&values) {
                    pending_indexes.push(entry);
                } else {
                    // `sql` is NULL (or unparseable). Every autoindex
                    // SQLite builds for a `PRIMARY KEY`/`UNIQUE`
                    // constraint has that shape, and dropping it here is
                    // what corrupts writes (#685) — so defer it and
                    // recover its key columns from the owning table's
                    // own DDL below.
                    pending_autoindexes.push((
                        text(values.get(2)).to_string(),
                        text(values.get(1)).to_string(),
                        match values.get(3) {
                            Some(Value::Integer(i)) => u32::try_from(*i).unwrap_or(0),
                            _ => 0,
                        },
                    ));
                }
            }
            "view" => views.push(ViewSchema {
                name: text(values.get(1)).to_string(),
                sql: text(values.get(4)).to_string(),
            }),
            _ => {}
        }
        row = cursor.next_row()?;
    }
    for (table_name, index) in pending_indexes {
        if let Some(schema) = table_pos
            .get(&table_name)
            .and_then(|&pos| schemas.get_mut(pos))
        {
            schema.indexes.push(index);
        }
    }
    for (table_name, index_name, root_page) in pending_autoindexes {
        let Some(&pos) = table_pos.get(&table_name) else {
            continue;
        };
        // Read the DDL out before taking the mutable borrow.
        let Some((sql, without_rowid)) = schemas
            .get(pos)
            .map(|schema| (schema.sql.clone(), schema.without_rowid))
        else {
            continue;
        };
        let resolved = autoindex_ordinal(&index_name, &table_name).and_then(|ordinal| {
            autoindex_key_lists(&sql, without_rowid)
                .and_then(|lists| lists.into_iter().nth(ordinal.saturating_sub(1)))
        });
        let Some(schema) = schemas.get_mut(pos) else {
            continue;
        };
        match resolved {
            Some(columns) => schema.indexes.push(IndexSchema {
                name: index_name,
                // Every `sqlite_autoindex_*` backs a `PRIMARY KEY` or
                // `UNIQUE` constraint, so it is always unique.
                unique: true,
                columns,
                root_page,
            }),
            // Could not recover the key. The index is real and on disk,
            // so the table must become read-only rather than writable
            // and silently corrupting.
            None => schema.unresolved_autoindex = true,
        }
    }
    Ok((schemas, views))
}

/// Walks `sqlite_master` via `cursor` (root_page = 1) and returns every
/// `type IN ('table', 'view')` name, unfiltered and in storage order —
/// the shell's `.tables` needs both kinds of names but none of
/// [`TableSchema`]'s DDL parsing, so this bypasses [`read_schema`]
/// entirely rather than teaching it a schema-kind discriminator.
pub fn read_table_and_view_names<P: PageSource>(
    cursor: &mut TableCursor<P>,
    encoding: TextEncoding,
) -> Result<Vec<String>, DdlError> {
    let mut names = Vec::new();
    let mut row = cursor.first_row()?;
    while let Some(r) = row {
        let values = decode_record(&r.payload, encoding)?;
        if values.len() != 5 {
            return Err(DdlError::MalformedRow(values.len()));
        }
        if matches!(text(values.first()), "table" | "view") {
            names.push(text(values.get(1)).to_string());
        }
        row = cursor.next_row()?;
    }
    Ok(names)
}

/// A minimally-decoded `sqlite_master` view entry (#380): just the
/// name and verbatim `CREATE VIEW ...` source text.
/// `codegen::expand_views` re-parses `sql` into a `CreateView` AST
/// (query + optional explicit column list) when it needs to substitute
/// a `FROM`-clause reference to this view — kept as a plain string here
/// rather than an eagerly-parsed AST so this module (per the module doc)
/// stays independent of the full SQL parser.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewSchema {
    /// The view's name.
    pub name: String,
    /// The verbatim `CREATE VIEW ...` source text.
    pub sql: String,
}

/// Walks `sqlite_master` via `cursor` (root_page = 1) and returns every
/// `type = 'view'` entry as a [`ViewSchema`] — the catalog counterpart
/// to [`read_schema`]'s tables, used by `codegen::expand_views` to
/// resolve a `FROM`-clause name against the view catalog before
/// falling back to a real table.
pub fn read_views<P: PageSource>(
    cursor: &mut TableCursor<P>,
    encoding: TextEncoding,
) -> Result<Vec<ViewSchema>, DdlError> {
    let mut views = Vec::new();
    let mut row = cursor.first_row()?;
    while let Some(r) = row {
        let values = decode_record(&r.payload, encoding)?;
        if values.len() != 5 {
            return Err(DdlError::MalformedRow(values.len()));
        }
        if text(values.first()) == "view" {
            views.push(ViewSchema {
                name: text(values.get(1)).to_string(),
                sql: text(values.get(4)).to_string(),
            });
        }
        row = cursor.next_row()?;
    }
    Ok(views)
}

fn table_schema(values: &[Value]) -> TableSchema {
    let name = text(values.get(1)).to_string();
    let root_page = match values.get(3) {
        Some(Value::Integer(i)) => *i as u32,
        _ => 0,
    };
    let sql = text(values.get(4));

    if is_virtual_table(sql) {
        return TableSchema {
            unresolved_autoindex: false,
            name,
            root_page: 0,
            columns: Vec::new(),
            without_rowid: false,
            strict: false,
            column_types: Vec::new(),
            column_collations: Vec::new(),
            is_virtual: true,
            sql: sql.to_string(),
            indexes: Vec::new(),
            rowid_alias: None,
        };
    }

    let parsed = parse_create_table(sql).unwrap_or_default();
    let rowid_alias = rowid_alias_from_sql(sql, parsed.without_rowid);
    TableSchema {
        unresolved_autoindex: false,
        name,
        root_page,
        columns: parsed.columns,
        without_rowid: parsed.without_rowid,
        strict: parsed.strict,
        column_types: parsed.column_types,
        column_collations: parsed.column_collations,
        is_virtual: false,
        sql: sql.to_string(),
        indexes: Vec::new(),
        rowid_alias,
    }
}

/// The ordinal `N` in `sqlite_autoindex_<table>_<N>` (#685).
fn autoindex_ordinal(index_name: &str, table_name: &str) -> Option<usize> {
    let prefix = format!("sqlite_autoindex_{table_name}_");
    index_name
        .strip_prefix(prefix.as_str())?
        .parse::<usize>()
        .ok()
        .filter(|n| *n >= 1)
}

/// Whether `def` contains `keyword` at paren depth 0, outside quotes.
///
/// `UNIQUE` inside `CHECK (x <> 'UNIQUE')` or inside a nested key list
/// must not count, which is why this scans the masked copy rather than
/// the raw text.
fn def_has_top_level_keyword(def: &str, keyword: &str) -> bool {
    let masked = mask_quotes_and_comments(def);
    let kw = keyword.as_bytes();
    let mut depth = 0i32;
    let mut i = 0usize;
    while i < masked.len() {
        match masked.get(i) {
            Some(b'(') => depth = depth.saturating_add(1),
            Some(b')') => depth = depth.saturating_sub(1),
            _ => {
                if depth == 0 {
                    if let Some(window) = masked.get(i..i.saturating_add(kw.len())) {
                        if window.eq_ignore_ascii_case(kw) {
                            return true;
                        }
                    }
                }
            }
        }
        i = i.saturating_add(1);
    }
    false
}

/// A table constraint's parenthesised key list as [`IndexedColumn`]s.
fn constraint_key_columns(constraint: &str) -> Option<Vec<IndexedColumn>> {
    let open = constraint.find('(')?;
    let close = constraint.rfind(')')?;
    if close <= open {
        return None;
    }
    let inner = constraint.get(open.saturating_add(1)..close)?;
    Some(
        split_top_level_commas(inner)
            .into_iter()
            .map(indexed_column)
            .collect(),
    )
}

/// `PRIMARY KEY` / `UNIQUE` / neither, for a table constraint, seeing
/// through an optional `CONSTRAINT <name>` prefix. `None` means "this
/// reader cannot tell", which the caller must treat as unresolvable
/// rather than as "no constraint".
fn table_constraint_kind(constraint: &str) -> Option<Option<bool>> {
    let mut rest = constraint.trim();
    if starts_with_ignore_case(rest, "CONSTRAINT") {
        // Skip `CONSTRAINT` and the identifier that follows it.
        let after = rest.get("CONSTRAINT".len()..)?.trim_start();
        let ident_end = after
            .find(|c: char| c.is_whitespace() || c == '(')
            .unwrap_or(after.len());
        rest = after.get(ident_end..)?.trim_start();
    }
    if starts_with_ignore_case(rest, "PRIMARY KEY") {
        return Some(Some(true));
    }
    if starts_with_ignore_case(rest, "UNIQUE") {
        return Some(Some(false));
    }
    if starts_with_ignore_case(rest, "FOREIGN KEY") || starts_with_ignore_case(rest, "CHECK") {
        return Some(None);
    }
    None
}

/// The rowid-alias column's name, using SQLite's actual rule.
///
/// Deliberately local rather than reusing [`rowid_alias_from_sql`]:
/// that function's table-level branch additionally requires the primary
/// key to name the table's *only* column, which is not SQLite's rule
/// (measured — `(a INTEGER, b TEXT, PRIMARY KEY (a))` is an alias). That
/// is a real read-correctness bug, but fixing it changes rowid handling
/// crate-wide, so #686 owns it; #685 must not smuggle that change in
/// alongside a corruption fix. When #686 lands, this collapses into a
/// call to the shared helper.
fn rowid_alias_column_name(sql: &str, without_rowid: bool) -> Option<String> {
    if without_rowid {
        return None;
    }
    let (start, end) = column_list_span(sql)?;
    let inner = sql.get(start..end)?;
    let mut columns: Vec<&str> = Vec::new();
    let mut constraints: Vec<&str> = Vec::new();
    for def in split_top_level_commas(inner) {
        if is_table_constraint(def) {
            constraints.push(def);
        } else {
            columns.push(def);
        }
    }
    for def in &columns {
        if is_integer_primary_key_inline(def) {
            return Some(column_name(def));
        }
    }
    // Table-level `PRIMARY KEY (col)`: an alias whenever it names one
    // INTEGER column, however many other columns the table has.
    for constraint in &constraints {
        if let Some(pk_col) = primary_key_single_column(constraint) {
            if columns
                .iter()
                .any(|def| is_integer_column(def) && column_name(def).eq_ignore_ascii_case(&pk_col))
            {
                return Some(pk_col);
            }
        }
    }
    None
}

/// Case-insensitive key-list equality, so a redundant constraint does
/// not claim a second autoindex number.
fn same_key_list(a: &[IndexedColumn], b: &[IndexedColumn]) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b.iter())
            .all(|(x, y)| x.name.eq_ignore_ascii_case(&y.name) && x.desc == y.desc)
}

/// Every key list SQLite creates a `sqlite_autoindex_*` for, in the
/// order it numbers them (#685).
///
/// The rule is oracle-derived (`sqlite3` 3.51.0), not inferred:
///
/// * declaration order wins — `UNIQUE (c), PRIMARY KEY (a, b)` numbers
///   the `UNIQUE` first;
/// * column-level `PRIMARY KEY`/`UNIQUE` count as much as table-level;
/// * a rowid-alias primary key gets no index *and consumes no number*;
/// * a `WITHOUT ROWID` table's primary key gets none — it *is* the table;
/// * two constraints over the same key list collapse to one index.
///
/// `None` means some constraint could not be understood. The caller must
/// treat that as unresolvable and refuse writes, never as "no index".
fn autoindex_key_lists(sql: &str, without_rowid: bool) -> Option<Vec<Vec<IndexedColumn>>> {
    let (start, end) = column_list_span(sql)?;
    let inner = sql.get(start..end)?;
    let alias = rowid_alias_column_name(sql, without_rowid);
    let mut out: Vec<Vec<IndexedColumn>> = Vec::new();
    let push = |cols: Vec<IndexedColumn>, out: &mut Vec<Vec<IndexedColumn>>| {
        if !out.iter().any(|existing| same_key_list(existing, &cols)) {
            out.push(cols);
        }
    };

    for def in split_top_level_commas(inner) {
        let trimmed = def.trim();
        if is_table_constraint(trimmed) {
            let kind = table_constraint_kind(trimmed)?;
            let Some(is_pk) = kind else {
                continue; // FOREIGN KEY / CHECK — no index
            };
            if is_pk && without_rowid {
                continue;
            }
            let cols = constraint_key_columns(trimmed)?;
            if cols.is_empty() {
                return None;
            }
            let is_alias = is_pk
                && cols.len() == 1
                && alias
                    .as_deref()
                    .is_some_and(|a| cols.first().is_some_and(|c| c.name.eq_ignore_ascii_case(a)));
            if is_alias {
                continue;
            }
            push(cols, &mut out);
        } else {
            let name = column_name(trimmed);
            if name.is_empty() {
                continue;
            }
            // A column-level constraint's index inherits the column's
            // declared `COLLATE`, which uniqueness checking depends on:
            // two byte-distinct values can be collation-equal, and the
            // autoindex SQLite built used the column's collation.
            let col = IndexedColumn {
                name: name.clone(),
                desc: false,
                collation: column_collation(trimmed),
            };
            let is_alias = alias
                .as_deref()
                .is_some_and(|a| a.eq_ignore_ascii_case(&name));
            if def_has_top_level_keyword(trimmed, "PRIMARY KEY") && !is_alias && !without_rowid {
                push(vec![col.clone()], &mut out);
            }
            if def_has_top_level_keyword(trimmed, "UNIQUE") {
                push(vec![col], &mut out);
            }
        }
    }
    Some(out)
}

/// Parses a `sqlite_master` row with `type = 'index'` into the owning
/// table's name and a naive [`IndexSchema`]. Returns `None` for anything
/// this naive reader can't find a column list in — most notably
/// auto-indexes (`sql` is `NULL` in `sqlite_master`) — treated identically
/// to unparseable table DDL: graceful, never an error.
fn index_schema(values: &[Value]) -> Option<(String, IndexSchema)> {
    let name = text(values.get(1)).to_string();
    let table_name = text(values.get(2)).to_string();
    let root_page = match values.get(3) {
        Some(Value::Integer(i)) => *i as u32,
        _ => 0,
    };
    let sql = text(values.get(4));

    let (start, end) = column_list_span(sql)?;
    let inner = sql.get(start..end)?;
    let columns = split_top_level_commas(inner)
        .into_iter()
        .map(indexed_column)
        .collect();

    Some((
        table_name,
        IndexSchema {
            name,
            unique: is_unique_index(sql),
            columns,
            root_page,
        },
    ))
}

/// Whether `s` starts with `prefix`, ASCII-case-insensitively, without
/// allocating an uppercased copy (#589). `prefix` must be ASCII (all
/// callers pass SQL keywords), so slicing `s` at `prefix.len()` either
/// lands on a char boundary or `get` returns `None` — never a panic.
fn starts_with_ignore_case(s: &str, prefix: &str) -> bool {
    s.get(..prefix.len())
        .is_some_and(|head| head.eq_ignore_ascii_case(prefix))
}

/// If `s` ends with `suffix` ASCII-case-insensitively, the byte index
/// where the suffix starts (i.e. `s.get(..idx)` strips it) —
/// allocation-free (#589), same ASCII-`suffix` char-boundary reasoning
/// as [`starts_with_ignore_case`].
fn suffix_start_ignore_case(s: &str, suffix: &str) -> Option<usize> {
    let split = s.len().checked_sub(suffix.len())?;
    s.get(split..)?
        .eq_ignore_ascii_case(suffix)
        .then_some(split)
}

/// Byte offset of the first ASCII-case-insensitive occurrence of
/// `needle` in `haystack`, allocation-free (#589) — the caller-facing
/// contract matches `haystack.to_ascii_uppercase().find(needle)` for an
/// ASCII `needle`.
fn find_ignore_case(haystack: &str, needle: &str) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    let end = haystack.len().checked_sub(needle.len())?;
    (0..=end).find(|&i| {
        haystack
            .get(i..i.saturating_add(needle.len()))
            .is_some_and(|window| window.eq_ignore_ascii_case(needle))
    })
}

fn is_unique_index(sql: &str) -> bool {
    starts_with_ignore_case(sql.trim_start(), "CREATE UNIQUE INDEX")
}

/// Splits a single indexed-column definition (`col`, `col DESC`, `col
/// COLLATE NOCASE ASC`, ...) into its column name and sort direction.
/// Expression indexes (`lower(col)`) are kept as raw text in `name` —
/// this reader doesn't parse expressions, matching the rest of this
/// module's "nothing more than the corpus needs" scope.
fn indexed_column(def: &str) -> IndexedColumn {
    let mut span = def.trim();
    let mut desc = false;
    if let Some(split) = suffix_start_ignore_case(span, " DESC") {
        desc = true;
        span = span.get(..split).unwrap_or(span).trim_end();
    } else if let Some(split) = suffix_start_ignore_case(span, " ASC") {
        span = span.get(..split).unwrap_or(span).trim_end();
    }

    let collation = column_collation(span);

    if let Some(pos) = find_ignore_case(span, " COLLATE ") {
        span = span.get(..pos).unwrap_or(span).trim_end();
    }

    IndexedColumn {
        name: span
            .trim_matches(['"', '`', '['].as_ref())
            .trim_matches([']'].as_ref())
            .to_string(),
        desc,
        collation,
    }
}

/// The [`Collation`] declared by a `COLLATE name` constraint anywhere in
/// `def` (a column definition or indexed-column spec), or
/// [`Collation::Binary`] when absent or unrecognized — matching SQLite's
/// own BINARY default.
fn column_collation(def: &str) -> Collation {
    let Some(pos) = find_ignore_case(def, "COLLATE") else {
        return Collation::Binary;
    };
    let rest = def
        .get(pos.saturating_add("COLLATE".len())..)
        .unwrap_or("")
        .trim_start();
    let name = rest
        .split_whitespace()
        .next()
        .unwrap_or("")
        .trim_matches(['"', '`', '['].as_ref())
        .trim_matches([']'].as_ref());
    if name.eq_ignore_ascii_case("NOCASE") {
        Collation::NoCase
    } else if name.eq_ignore_ascii_case("RTRIM") {
        Collation::RTrim
    } else {
        Collation::Binary
    }
}

fn text(v: Option<&Value>) -> &str {
    match v {
        Some(Value::Text(s)) => s,
        _ => "",
    }
}

fn is_virtual_table(sql: &str) -> bool {
    starts_with_ignore_case(sql.trim_start(), "CREATE VIRTUAL TABLE")
}

#[derive(Default)]
struct ParsedCreateTable {
    columns: Vec<String>,
    column_types: Vec<String>,
    column_collations: Vec<Collation>,
    without_rowid: bool,
    strict: bool,
}

/// Parses `CREATE TABLE ... (col-defs) [table-options]`. Returns `None`
/// for anything this naive reader can't find a column list in — the
/// caller treats that identically to a virtual table (empty columns,
/// never an error).
fn parse_create_table(sql: &str) -> Option<ParsedCreateTable> {
    let (start, end) = column_list_span(sql)?;
    let inner = sql.get(start..end)?;
    let trailer = sql.get(end.saturating_add(1)..)?.to_ascii_uppercase();

    let defs: Vec<&str> = split_top_level_commas(inner)
        .into_iter()
        .filter(|def| !is_table_constraint(def))
        .collect();
    let columns = defs.iter().map(|def| column_name(def)).collect();
    let column_types = defs.iter().map(|def| column_type(def)).collect();
    let column_collations = defs.iter().map(|def| column_collation(def)).collect();

    Some(ParsedCreateTable {
        columns,
        column_types,
        column_collations,
        without_rowid: trailer.contains("WITHOUT ROWID"),
        strict: trailer.contains("STRICT"),
    })
}

/// A standalone table-level constraint (`PRIMARY KEY(...)`, `UNIQUE(...)`,
/// `FOREIGN KEY(...)`, `CHECK(...)`, `CONSTRAINT name ...`) rather than a
/// column definition. Inline column-level constraints (e.g. `k PRIMARY
/// KEY`) don't start with the keyword — the column name comes first —
/// so this check doesn't false-positive on those.
fn is_table_constraint(def: &str) -> bool {
    let trimmed = def.trim();
    starts_with_ignore_case(trimmed, "PRIMARY KEY")
        || starts_with_ignore_case(trimmed, "UNIQUE")
        || starts_with_ignore_case(trimmed, "FOREIGN KEY")
        || starts_with_ignore_case(trimmed, "CHECK")
        || starts_with_ignore_case(trimmed, "CONSTRAINT")
}

/// Replaces the contents of quoted regions (`'...'`, `"..."`,
/// `` `...` ``, `[...]`) and comments (`--...`, `/*...*/`) with spaces,
/// preserving every other byte and the overall byte length — so a
/// masked offset always lines up with the same offset in the original
/// string. Callers that need to find top-level structure (parens,
/// commas, keywords) scan the masked bytes but slice the *original*
/// string for the text they return, so quoted content survives intact
/// while it can no longer be mistaken for syntax (#135: a comma or the
/// words PRIMARY/KEY inside a string literal used to be indistinguishable
/// from the real thing).
fn mask_quotes_and_comments(s: &str) -> Vec<u8> {
    let bytes = s.as_bytes();
    let mut out = bytes.to_vec();
    let mut i = 0usize;
    let at = |i: usize| bytes.get(i).copied();
    let blank = |out: &mut [u8], i: usize| {
        if let Some(b) = out.get_mut(i) {
            *b = b' ';
        }
    };
    while let Some(here) = at(i) {
        match here {
            b'\'' | b'"' => {
                let quote = here;
                blank(&mut out, i);
                i = i.saturating_add(1);
                while let Some(b) = at(i) {
                    if b == quote {
                        blank(&mut out, i);
                        i = i.saturating_add(1);
                        // A doubled quote (`''`/`""`) is an escaped
                        // literal quote character, not the closing one.
                        if at(i) == Some(quote) {
                            blank(&mut out, i);
                            i = i.saturating_add(1);
                            continue;
                        }
                        break;
                    }
                    blank(&mut out, i);
                    i = i.saturating_add(1);
                }
            }
            b'`' | b'[' => {
                let close = if here == b'`' { b'`' } else { b']' };
                blank(&mut out, i);
                i = i.saturating_add(1);
                while let Some(b) = at(i) {
                    blank(&mut out, i);
                    i = i.saturating_add(1);
                    if b == close {
                        break;
                    }
                }
            }
            b'-' if at(i.saturating_add(1)) == Some(b'-') => {
                while let Some(b) = at(i) {
                    if b == b'\n' {
                        break;
                    }
                    blank(&mut out, i);
                    i = i.saturating_add(1);
                }
            }
            b'/' if at(i.saturating_add(1)) == Some(b'*') => {
                blank(&mut out, i);
                blank(&mut out, i.saturating_add(1));
                i = i.saturating_add(2);
                while let Some(b) = at(i) {
                    if b == b'*' && at(i.saturating_add(1)) == Some(b'/') {
                        blank(&mut out, i);
                        blank(&mut out, i.saturating_add(1));
                        i = i.saturating_add(2);
                        break;
                    }
                    blank(&mut out, i);
                    i = i.saturating_add(1);
                }
            }
            _ => i = i.saturating_add(1),
        }
    }
    out
}

/// Byte range *between* the outer parens of a `CREATE TABLE`'s
/// column-definition list, or `None` when there is no balanced list.
/// Paren depth is tracked over the quote/comment-masked text (#135) so a
/// paren inside a string literal can't unbalance the scan, but the
/// returned range indexes the original `sql` string.
fn column_list_span(sql: &str) -> Option<(usize, usize)> {
    let masked = mask_quotes_and_comments(sql);
    let start = masked.iter().position(|&b| b == b'(')?;
    let mut depth = 0i32;
    for (offset, &b) in masked.get(start..)?.iter().enumerate() {
        match b {
            b'(' => depth = depth.saturating_add(1),
            b')' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some((start.saturating_add(1), start.saturating_add(offset)));
                }
            }
            _ => {}
        }
    }
    None
}

/// The single top-level-comma splitter behind both
/// [`parse_create_table`]'s `columns` and [`column_defs`]. These two
/// MUST agree position-for-position: [`rowid_alias_from_sql`] returns an
/// index into `column_defs`, and `src/codegen/expr.rs` resolves that
/// index against `TableSchema::columns`. Two copies of this loop would
/// let the lists drift and silently mis-target the rowid substitution,
/// so they share one implementation rather than mirroring each other.
/// Splits over the quote/comment-masked text (#135) so a comma inside a
/// string literal (e.g. `DEFAULT 'a,b'`) is not mistaken for a column
/// separator; the returned slices still come from the original `inner`.
fn split_top_level_commas(inner: &str) -> Vec<&str> {
    let masked = mask_quotes_and_comments(inner);
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut part_start = 0usize;
    for (i, &b) in masked.iter().enumerate() {
        match b {
            b'(' => depth = depth.saturating_add(1),
            b')' => depth = depth.saturating_sub(1),
            b',' if depth == 0 => {
                parts.push(inner.get(part_start..i).unwrap_or("").trim());
                part_start = i.saturating_add(1);
            }
            _ => {}
        }
    }
    parts.push(inner.get(part_start..).unwrap_or("").trim());
    parts
}

fn column_name(def: &str) -> String {
    def.split_whitespace()
        .next()
        .unwrap_or("")
        .trim_matches(['"', '`', '['].as_ref())
        .trim_matches([']'].as_ref())
        .to_string()
}

/// The declared-type text between a column's name and its first
/// column-constraint keyword — SQLite's own notion of "declared type"
/// (datatype3.html §3.1), which may span several words (`DOUBLE
/// PRECISION`, `UNSIGNED BIG INT`). Empty when the column has no type
/// (`CREATE TABLE t(x)`).
pub fn column_type(def: &str) -> String {
    const CONSTRAINT_KEYWORDS: [&str; 8] = [
        "PRIMARY",
        "NOT",
        "NULL",
        "UNIQUE",
        "CHECK",
        "DEFAULT",
        "COLLATE",
        "REFERENCES",
    ];
    def.split_whitespace()
        .skip(1)
        .take_while(|word| {
            !CONSTRAINT_KEYWORDS.iter().any(|kw| {
                word.eq_ignore_ascii_case(kw)
                    || (starts_with_ignore_case(word, kw)
                        && word.as_bytes().get(kw.len()) == Some(&b'('))
            })
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Splits a `CREATE TABLE ...(col-defs)...` statement's column-definition
/// list into raw per-column definition strings, in declared order —
/// re-derived from `schema.sql` rather than kept alongside `columns`,
/// which holds names only — `src/dump.rs` needs each column's declared
/// type text, and [`rowid_alias_from_sql`] needs its full constraint
/// text. Shares [`split_top_level_commas`] and [`is_table_constraint`]
/// with [`parse_create_table`], so the two column lists cannot drift.
pub fn column_defs(schema: &TableSchema) -> Vec<&str> {
    all_defs(schema)
        .into_iter()
        .filter(|def| !is_table_constraint(def))
        .collect()
}

/// Every top-level def in the column list, column definitions and
/// table-level constraints alike — the raw material [`column_defs`]
/// filters down and [`rowid_alias_from_sql`] additionally needs the
/// constraint side of (to recognize a table-level `PRIMARY KEY(col)`).
fn all_defs(schema: &TableSchema) -> Vec<&str> {
    let Some((start, end)) = column_list_span(&schema.sql) else {
        return Vec::new();
    };
    let Some(inner) = schema.sql.get(start..end) else {
        return Vec::new();
    };
    split_top_level_commas(inner)
}

/// Whether `def` is a column definition carrying an inline
/// `INTEGER PRIMARY KEY` (excluding the `DESC` form, which SQLite does
/// not treat as a rowid alias). Scans the quote/comment-masked text
/// (#135) so a string literal mentioning "primary key" — e.g.
/// `DEFAULT 'primary key'` — can't be mistaken for the real constraint.
fn is_integer_primary_key_inline(def: &str) -> bool {
    let masked = mask_quotes_and_comments(def);
    let masked = std::str::from_utf8(&masked).unwrap_or_default();
    let is_pk = masked
        .split(|c: char| !c.is_alphanumeric())
        .collect::<Vec<_>>()
        .windows(2)
        .any(|w| matches!(w, [a, b] if a.eq_ignore_ascii_case("PRIMARY") && b.eq_ignore_ascii_case("KEY")));
    is_pk
        && masked
            .split_whitespace()
            .any(|w| w.eq_ignore_ascii_case("INTEGER"))
        // `INTEGER PRIMARY KEY DESC` is deliberately NOT a rowid
        // alias in SQLite — the DESC form gets its own b-tree index
        // and the column is stored normally, so substituting the
        // cursor's rowid would return values that aren't there.
        && !masked
            .split_whitespace()
            .any(|w| w.eq_ignore_ascii_case("DESC"))
}

/// Whether `def` declares an `INTEGER` type (masked, so a string literal
/// can't supply the word), regardless of any inline constraint.
fn is_integer_column(def: &str) -> bool {
    let masked = mask_quotes_and_comments(def);
    let masked = std::str::from_utf8(&masked).unwrap_or_default();
    masked
        .split_whitespace()
        .any(|w| w.eq_ignore_ascii_case("INTEGER"))
}

/// If `constraint` is a table-level `PRIMARY KEY(col)` naming exactly
/// one column, returns that column's (unquoted) name.
fn primary_key_single_column(constraint: &str) -> Option<String> {
    if !starts_with_ignore_case(constraint.trim_start(), "PRIMARY KEY") {
        return None;
    }
    let open = constraint.find('(')?;
    let close = constraint.rfind(')')?;
    if close <= open {
        return None;
    }
    let inner = constraint.get(open.saturating_add(1)..close)?;
    let cols = split_top_level_commas(inner);
    match cols.as_slice() {
        [only] => Some(column_name(only)),
        _ => None,
    }
}

/// The one-column special case SQLite calls the rowid alias: a table
/// declared with a single `INTEGER PRIMARY KEY` column (not `WITHOUT
/// ROWID`) stores that column as a NULL placeholder in every record and
/// expects the reader to substitute the cursor's own rowid instead (see
/// `src/btree/mod.rs`'s module doc and spike 003 finding 1). Returns the
/// 0-based column index to substitute, if any.
///
/// Detection is textual and shares this module's documented naivety —
/// see the `known_fragile_*` tests in `src/dump.rs` for the two forms it
/// still gets wrong. That matters more than it used to:
/// `src/codegen/expr.rs` now emits `Rowid` instead of `Column` based on
/// this answer, so a wrong index is a wrong query result, not just wrong
/// `dump` output.
///
/// Runs once per table at schema-decode time ([`TableSchema::rowid_alias`]
/// caches the answer); everything after decode reads the field (#589).
pub fn rowid_alias_from_sql(sql: &str, without_rowid: bool) -> Option<usize> {
    if without_rowid {
        return None;
    }
    let (start, end) = column_list_span(sql)?;
    let inner = sql.get(start..end)?;
    // Partition column defs from table-level constraints in one pass —
    // the pre-#589 code derived each list with its own full re-parse.
    let mut columns: Vec<&str> = Vec::new();
    let mut constraints: Vec<&str> = Vec::new();
    for def in split_top_level_commas(inner) {
        if is_table_constraint(def) {
            constraints.push(def);
        } else {
            columns.push(def);
        }
    }
    for (idx, def) in columns.iter().enumerate() {
        if is_integer_primary_key_inline(def) {
            return Some(idx);
        }
    }
    // The table-level `PRIMARY KEY(col)` form: SQLite only treats this
    // as a rowid alias when it names the table's one and only column,
    // and that column is INTEGER-typed (a composite key, or a second
    // column, rules it out).
    if let [only] = columns.as_slice() {
        if is_integer_column(only) {
            let col_name = column_name(only);
            let is_alias = constraints
                .iter()
                .filter_map(|c| primary_key_single_column(c))
                .any(|pk_col| pk_col.eq_ignore_ascii_case(&col_name));
            if is_alias {
                return Some(0);
            }
        }
    }
    None
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
    use crate::vfs::{UnixVfs, Vfs, VfsPageSource};
    use std::path::Path;

    fn read_fixture(family: &str, name: &str) -> Vec<TableSchema> {
        let path = Path::new("tests/corpus/fixtures").join(family).join(name);
        let vfs = UnixVfs;
        let file = vfs
            .open_read(&path)
            .unwrap_or_else(|e| panic!("open {path:?}: {e}"));
        let mut header_buf = [0u8; 100];
        file.read_at(&mut header_buf, 0).unwrap();
        let header = DatabaseHeader::parse(&header_buf).unwrap();
        let source = VfsPageSource::open(&vfs, &path, header.page_size).unwrap();
        let mut cursor = TableCursor::new(source, &header, 1);
        read_schema(&mut cursor, header.text_encoding).unwrap()
    }

    fn find(schemas: &[TableSchema], name: &str) -> TableSchema {
        schemas
            .iter()
            .find(|s| s.name == name)
            .cloned()
            .unwrap_or_else(|| panic!("no table named {name} in {schemas:?}"))
    }

    #[test]
    fn plain_table_columns_and_rootpage() {
        let schemas = read_fixture("btrees", "table_single_page.db");
        assert_eq!(schemas.len(), 1);
        let t = find(&schemas, "t");
        assert_eq!(t.columns, vec!["a", "b"]);
        assert_eq!(t.root_page, 2);
        assert!(!t.without_rowid);
        assert!(!t.strict);
        assert!(!t.is_virtual);
    }

    #[test]
    fn without_rowid_marker_detected() {
        let schemas = read_fixture("btrees", "without_rowid.db");
        let t = find(&schemas, "t");
        assert_eq!(t.columns, vec!["k", "v"]);
        assert!(t.without_rowid);
    }

    #[test]
    fn strict_marker_detected_and_generated_column_named_correctly() {
        let schemas = read_fixture("features", "strict_generated.db");
        let t = find(&schemas, "t");
        // `b INTEGER GENERATED ALWAYS AS (a*2) STORED` — the naive
        // column-name extractor must not be confused by the nested
        // parens in the generated-column expression.
        assert_eq!(t.columns, vec!["a", "b"]);
        assert!(t.strict);
        assert!(!t.without_rowid);
    }

    #[test]
    fn autovacuum_fixture_plain_table_unaffected_by_pointer_map_page() {
        let schemas = read_fixture("features", "autovacuum.db");
        let t = find(&schemas, "t");
        assert_eq!(t.columns, vec!["a", "b"]);
    }

    #[test]
    fn fts5_virtual_table_is_graceful_unknown_shadow_tables_are_readable() {
        let schemas = read_fixture("features", "fts5.db");

        let virtual_entry = find(&schemas, "t");
        assert!(virtual_entry.is_virtual);
        assert!(virtual_entry.columns.is_empty());
        assert_eq!(virtual_entry.root_page, 0);

        // Shadow tables are ordinary sqlite_master table entries and
        // must be fully readable, including the WITHOUT ROWID ones
        // (spike 005, #12's finding that motivated spec 006 Req 5/6).
        assert_eq!(find(&schemas, "t_data").columns, vec!["id", "block"]);
        assert!(!find(&schemas, "t_data").without_rowid);

        let t_idx = find(&schemas, "t_idx");
        // `(segid, term, pgno, PRIMARY KEY(segid, term))` — the
        // standalone table-level PRIMARY KEY(...) constraint must not be
        // mistaken for a 4th column named "PRIMARY".
        assert_eq!(t_idx.columns, vec!["segid", "term", "pgno"]);
        assert!(t_idx.without_rowid);

        assert_eq!(find(&schemas, "t_content").columns, vec!["id", "c0"]);
        assert_eq!(find(&schemas, "t_docsize").columns, vec!["id", "sz"]);

        let t_config = find(&schemas, "t_config");
        // `(k PRIMARY KEY, v)` — inline column-level PRIMARY KEY on `k`
        // must still name the column "k", not be filtered out.
        assert_eq!(t_config.columns, vec!["k", "v"]);
        assert!(t_config.without_rowid);
    }

    #[test]
    fn rtree_virtual_table_is_graceful_unknown_shadow_tables_are_readable() {
        let schemas = read_fixture("features", "rtree.db");

        let virtual_entry = find(&schemas, "t");
        assert!(virtual_entry.is_virtual);
        assert!(virtual_entry.columns.is_empty());

        assert_eq!(find(&schemas, "t_rowid").columns, vec!["rowid", "nodeno"]);
        assert_eq!(find(&schemas, "t_node").columns, vec!["nodeno", "data"]);
        assert_eq!(
            find(&schemas, "t_parent").columns,
            vec!["nodeno", "parentnode"]
        );
    }

    #[test]
    fn unparseable_non_virtual_ddl_degrades_gracefully_never_errors() {
        // Synthetic row: no column-list parens at all. Not a real
        // fixture case (every real CREATE TABLE has a paren list), but
        // exercises the "never an error" guarantee for anything this
        // naive reader genuinely can't make sense of.
        let values = vec![
            Value::Text("table".to_string().into()),
            Value::Text("weird".to_string().into()),
            Value::Text("weird".to_string().into()),
            Value::Integer(5),
            Value::Text("garbage with no parens".to_string().into()),
        ];
        let schema = table_schema(&values);
        assert_eq!(schema.name, "weird");
        assert_eq!(schema.root_page, 5);
        assert!(schema.columns.is_empty());
        assert!(!schema.is_virtual);
    }

    #[test]
    fn non_table_entries_are_excluded() {
        // index.db has an explicit secondary index (idx_b) alongside
        // table t — read_schema must return only tables as top-level
        // entries, with idx_b attached to t's `indexes` (#211).
        let schemas = read_fixture("btrees", "index.db");
        assert_eq!(schemas.len(), 1);
        assert_eq!(schemas[0].name, "t");
        assert_eq!(schemas[0].indexes.len(), 1);
        assert_eq!(schemas[0].indexes[0].name, "idx_b");
        assert!(!schemas[0].indexes[0].unique);
        assert_eq!(schemas[0].indexes[0].columns.len(), 1);
        assert_eq!(schemas[0].indexes[0].columns[0].name, "b");
        assert!(!schemas[0].indexes[0].columns[0].desc);
    }

    #[test]
    fn unique_index_with_multiple_columns_and_desc() {
        let sql = "CREATE UNIQUE INDEX idx_ab ON t(a DESC, b)";
        let (table_name, index) = index_schema(&[
            Value::Null,
            Value::Text("idx_ab".to_string().into()),
            Value::Text("t".to_string().into()),
            Value::Integer(0),
            Value::Text(sql.to_string().into()),
        ])
        .expect("index_schema should parse a simple CREATE UNIQUE INDEX");
        assert_eq!(table_name, "t");
        assert!(index.unique);
        assert_eq!(index.columns.len(), 2);
        assert_eq!(index.columns[0].name, "a");
        assert!(index.columns[0].desc);
        assert_eq!(index.columns[1].name, "b");
        assert!(!index.columns[1].desc);
    }

    #[test]
    fn auto_index_with_null_sql_is_omitted() {
        // Auto-indexes created for PRIMARY KEY/UNIQUE constraints have a
        // NULL `sql` column in sqlite_master — this naive reader can't
        // find a column list in an empty string, so it's gracefully
        // skipped rather than erroring.
        let result = index_schema(&[
            Value::Null,
            Value::Text("sqlite_autoindex_t_1".to_string().into()),
            Value::Text("t".to_string().into()),
            Value::Integer(0),
            Value::Null,
        ]);
        assert!(result.is_none());
    }
}
