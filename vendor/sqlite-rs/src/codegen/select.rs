// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! `Select` AST -> `Program` compilation (spec 009, Requirement 11's
//! surrounding statement shape): `Init -> OpenRead -> Rewind -> [WHERE
//! test, result columns, ResultRow] -> Next -> Halt`, with ORDER BY
//! wired through the sorter opcodes, LIMIT/OFFSET as independent
//! `IfPos`/`DecrJumpZero` counters, and DISTINCT via the in-memory
//! ephemeral index — mirroring `tests/unit/vdbe_cursor_sorter_test.rs`'s
//! hand-assembled shapes.
//!
//! Known simplification: LIMIT/OFFSET compile to two independent
//! counters (`IfPos` to skip the first OFFSET matching rows, then
//! `DecrJumpZero` to stop after LIMIT rows) rather than the single
//! combined budget register `OffsetLimit` computes — `OffsetLimit`
//! itself was already implemented and tested by #89; this ticket just
//! doesn't happen to need it for a correct LIMIT/OFFSET shape.

use crate::codegen::expr::{
    collation_of, column_index, compile_cond, compile_value, emit_column_read, expr_affinity,
    expr_collation, is_aggregate_call,
};
use crate::codegen::{
    p4_coll_seq, CondTargets, Emitter, Label, RegAlloc, Scope, TableBinding, Target,
};
use crate::parser::ast::{
    BinaryOp, CompoundOp, CompoundSelect, Distinctness, Expr, ExprKind, FromClause, FunctionArgs,
    JoinConstraint, JoinOp, Literal, ParamKind, ResultColumn, Select, TableRef, TableRefKind,
};
use crate::parser::tokenizer::Span;
use crate::schema::{IndexSchema, TableSchema};
use crate::vdbe::{
    comparison_affinity, Affinity, Collation, Instruction, Opcode, Program, SortKeyColumn, P4,
};

/// Errors raised while compiling a `SELECT` (or a statement that embeds one,
/// e.g. `INSERT ... SELECT`) into a `Program`.
#[derive(Debug, PartialEq, Eq)]
pub enum CodegenError {
    /// The statement has no `FROM` clause, which this compiler doesn't
    /// support.
    NoFromClause,

    /// A referenced column name doesn't resolve against any table in scope.
    UnknownColumn {
        /// The column name that failed to resolve.
        name: String,
    },

    /// #237: an unqualified column name in a multi-table `FROM` matched
    /// more than one joined table's schema.
    AmbiguousColumn {
        /// The unqualified column name that matched more than one joined
        /// table's schema.
        name: String,
    },

    /// A construct recognized by the parser but not (yet) handled by this
    /// compiler.
    Unsupported {
        /// Human-readable description of the unsupported construct.
        reason: String,
    },

    /// #195: an `INSERT` row supplied a different number of values than
    /// the target column list names.
    RowShapeMismatch {
        /// Name of the target table.
        table: String,
        /// Number of columns the target table (or column list) expects.
        expected: usize,
        /// Number of values actually supplied by the row.
        found: usize,
    },

    /// #240: a `UNION ALL` arm projected a different number of result
    /// columns than the first arm — SQLite rejects this at compile time
    /// rather than padding/truncating rows.
    CompoundColumnMismatch {
        /// Number of result columns projected by the first compound arm.
        expected: usize,
        /// Number of result columns projected by the mismatched arm.
        found: usize,
    },

    /// #380 follow-up: a view (directly or transitively, via other
    /// views) references itself in its own `FROM`/`JOIN` clause. Message
    /// matches stock SQLite's own wording (`view {name} is circularly
    /// defined`) for oracle-diff parity.
    CircularView {
        /// Name of the view that references itself.
        name: String,
    },
}

impl std::fmt::Display for CodegenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoFromClause => {
                write!(
                    f,
                    "SELECT has no FROM clause — not supported by this V2-scope compiler"
                )
            }
            Self::UnknownColumn { name } => write!(f, "unknown column {name:?}"),
            Self::AmbiguousColumn { name } => write!(f, "ambiguous column name: {name:?}"),
            Self::Unsupported { reason } => write!(f, "unsupported: {reason}"),
            Self::RowShapeMismatch {
                table,
                expected,
                found,
            } => {
                write!(
                    f,
                    "{table} has {expected} columns but {found} values were supplied"
                )
            }
            Self::CompoundColumnMismatch { expected, found } => write!(
                f,
                "SELECTs to the left and right of UNION ALL do not have the same number of \
                 result columns: expected {expected}, found {found}"
            ),
            Self::CircularView { name } => write!(f, "view {name} is circularly defined"),
        }
    }
}

impl std::error::Error for CodegenError {}

const TABLE_CURSOR: i32 = 0;
const SORT_CURSOR: i32 = 1;
const PSEUDO_CURSOR: i32 = 2;
const DISTINCT_CURSOR: i32 = 3;

/// The scan's cursor numbers, parameterized (rather than the fixed
/// `TABLE_CURSOR`/`SORT_CURSOR`/`PSEUDO_CURSOR`/`DISTINCT_CURSOR`
/// constants) so [`compile_select_scan`] can be embedded inside another
/// statement's program (#208: `INSERT ... SELECT`) without colliding
/// with that statement's own cursor numbers.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ScanCursors {
    pub(crate) table: i32,
    pub(crate) sort: i32,
    pub(crate) pseudo: i32,
    pub(crate) distinct: i32,
}

impl ScanCursors {
    const fn for_standalone_select() -> Self {
        Self {
            table: TABLE_CURSOR,
            sort: SORT_CURSOR,
            pseudo: PSEUDO_CURSOR,
            distinct: DISTINCT_CURSOR,
        }
    }

    /// One full, non-colliding cursor set per `UNION ALL` arm (#240) —
    /// `index` 0 is the compound's first arm, 1.. are `select.compound`
    /// arms in order. Each arm gets 4 cursor numbers to itself so an
    /// arm using its own ORDER BY sort cursor or DISTINCT ephemeral
    /// index never collides with another arm's.
    const fn for_arm(index: usize) -> Self {
        let base = (index as i32).saturating_mul(4);
        Self {
            table: base,
            sort: base.saturating_add(1),
            pseudo: base.saturating_add(2),
            distinct: base.saturating_add(3),
        }
    }

    /// First cursor number past `count` arms' worth of
    /// [`Self::for_arm`] blocks — the single block size (4 cursors)
    /// lives here so a caller allocating a cursor after all arm blocks
    /// (e.g. the UNION dedup ephemeral index in `select/entry.rs`)
    /// never hardcodes it independently.
    pub(crate) fn after_arms(count: usize) -> i32 {
        i32::try_from(count).unwrap_or(i32::MAX).saturating_mul(4)
    }
}

mod aggregate;
mod entry;
mod eqp;
mod index_scan;
pub(super) mod join_access;
mod join_full;
mod join_order;
mod joins;
mod limit_scan;
mod order_by;
mod projection;
mod range_scan;

pub use entry::{
    compile_select, compile_select_compound, compile_select_with_catalog,
    compile_select_with_catalog_and_stats,
};
pub use eqp::{explain_query_plan, EqpRow};
pub use joins::compile_select_joined;
pub use order_by::output_column_names;

pub(crate) use aggregate::{
    compile_grouped_scan, select_has_aggregate, try_compile_index_only_count,
    try_compile_index_only_sum,
};
pub(crate) use entry::{
    compile_select_scan, select_result_column_count, select_result_column_count_joined,
};
pub(crate) use joins::compile_select_joined_scan;
pub(crate) use limit_scan::{is_rowid_reference, top_level_equality_operands};
pub(crate) use range_scan::{range_seek_index_position, try_compile_range_row_seek};

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    #[test]
    fn codegen_error_display_variants() {
        assert_eq!(
            CodegenError::NoFromClause.to_string(),
            "SELECT has no FROM clause — not supported by this V2-scope compiler"
        );
        assert_eq!(
            CodegenError::UnknownColumn {
                name: "x".to_string()
            }
            .to_string(),
            "unknown column \"x\""
        );
        assert_eq!(
            CodegenError::AmbiguousColumn {
                name: "x".to_string()
            }
            .to_string(),
            "ambiguous column name: \"x\""
        );
        assert_eq!(
            CodegenError::Unsupported {
                reason: "foo".to_string()
            }
            .to_string(),
            "unsupported: foo"
        );
        assert_eq!(
            CodegenError::RowShapeMismatch {
                table: "t".to_string(),
                expected: 2,
                found: 3,
            }
            .to_string(),
            "t has 2 columns but 3 values were supplied"
        );
        assert_eq!(
            CodegenError::CompoundColumnMismatch {
                expected: 2,
                found: 3,
            }
            .to_string(),
            "SELECTs to the left and right of UNION ALL do not have the same number of result \
             columns: expected 2, found 3"
        );
        assert_eq!(
            CodegenError::CircularView {
                name: "v".to_string()
            }
            .to_string(),
            "view v is circularly defined"
        );
    }

    #[test]
    fn codegen_error_is_std_error() {
        let err = CodegenError::NoFromClause;
        assert!(std::error::Error::source(&err).is_none());
    }

    #[test]
    fn scan_cursors_for_standalone_select() {
        let cursors = ScanCursors::for_standalone_select();
        assert_eq!(cursors.table, TABLE_CURSOR);
        assert_eq!(cursors.sort, SORT_CURSOR);
        assert_eq!(cursors.pseudo, PSEUDO_CURSOR);
        assert_eq!(cursors.distinct, DISTINCT_CURSOR);
    }

    #[test]
    fn scan_cursors_for_arm_offsets_by_four() {
        let arm0 = ScanCursors::for_arm(0);
        assert_eq!(
            (arm0.table, arm0.sort, arm0.pseudo, arm0.distinct),
            (0, 1, 2, 3)
        );
        let arm1 = ScanCursors::for_arm(1);
        assert_eq!(
            (arm1.table, arm1.sort, arm1.pseudo, arm1.distinct),
            (4, 5, 6, 7)
        );
    }

    #[test]
    fn scan_cursors_after_arms() {
        assert_eq!(ScanCursors::after_arms(0), 0);
        assert_eq!(ScanCursors::after_arms(3), 12);
    }
}
