// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! SQL parser (spec 002-parser). Lives independently of
//! `src/schema/ddl_reader.rs` (Requirement 5): the minimal DDL reader
//! must keep building and passing with this module absent, so nothing
//! under `src/schema` may depend on anything here.

pub mod ast;
pub mod error;
pub mod grammar;
pub mod printer;
pub mod tokenizer;

pub use error::{
    parse_begin, parse_commit, parse_create_index, parse_create_table, parse_create_view,
    parse_delete, parse_drop_index, parse_drop_table, parse_drop_view, parse_explain, parse_insert,
    parse_pragma, parse_rollback, parse_select, parse_update, ParseOutcome,
};
pub use tokenizer::{ends_with_semicolon, split_statements};
