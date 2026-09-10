// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! Minimal schema reader (Tier 0): decodes `sqlite_master` into enough
//! structure to drive the table/index b-tree cursors, without depending
//! on the (future) full SQL parser. See `.openspec/specs/002-parser/spec.md`
//! Requirement 5 — this module lives outside `src/parser/` by design.

mod ddl_reader;

pub use ddl_reader::{
    column_defs, column_type, read_schema, read_schema_and_views, read_table_and_view_names,
    read_views, rowid_alias_from_sql, DdlError, IndexSchema, IndexedColumn, TableSchema,
    ViewSchema,
};
