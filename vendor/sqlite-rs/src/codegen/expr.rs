// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! Expression lowering (spec 009, Requirement 11): boolean-valued
//! expressions compile to jump instructions targeting a true/false
//! continuation, never an intermediate boolean register — the classic
//! jumping-code-generation technique. `compile_cond` is the jump-mode
//! entry point; `compile_value` is the ordinary register-producing
//! entry point used for result columns, function arguments, and CASE
//! branch results.
//!
//! Every column reference resolves through a [`Scope`] (#237) rather
//! than a bare `schema: &TableSchema, cursor: i32` pair — the single-
//! table V2 case is just `Scope::single(schema, cursor)`; a join chain
//! is `Scope` with one [`crate::codegen::TableBinding`] per joined
//! table, and `table.column`/bare `column` references resolve against
//! whichever binding matches (see `Scope::resolve`'s doc comment for
//! the alias-vs-name precedence rule).
//!
//! Split (#339, follow-up to #273/#329) into [`cond`] (jump-mode:
//! `compile_cond` and its false-jump/label helpers) and [`value`]
//! (value-mode: `compile_value` and everything it shares with jump
//! mode, e.g. column reads and affinity/collation lookups).

mod cond;
mod value;

pub(crate) use cond::{column_index, compile_cond, ensure_label};
pub(crate) use value::{
    collation_of, compile_value, emit_column_read, expr_affinity, expr_collation, is_aggregate_call,
};
