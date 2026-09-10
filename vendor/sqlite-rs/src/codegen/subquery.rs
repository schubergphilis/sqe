// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! Subquery-expression codegen (#238, plus the correlated-subquery
//! follow-up): scalar subqueries (`(SELECT ...)`), `IN (SELECT ...)`/
//! `NOT IN (SELECT ...)`, and `EXISTS (SELECT ...)`/`NOT EXISTS
//! (SELECT ...)`. Materialization only (no coroutines) — each subquery
//! occurrence opens its own table cursor (and, for `IN`, an ephemeral
//! index to hold the materialized result column) via
//! [`RegAlloc::alloc_cursor`], compiles the inner `SELECT`'s own
//! single-table scan inline into the enclosing instruction stream, and
//! either captures its first row's leading column (scalar subquery) or
//! tests row existence (`EXISTS`) or row membership (`IN`).
//!
//! Correlation (a column reference inside the subquery that resolves
//! against the *enclosing* query's scope rather than the subquery's
//! own) works for free under materialization: the subquery's own
//! `Scope` is built with [`Scope::with_outer`] pointing at the
//! enclosing scope, so [`Scope::resolve`] falls back there for any
//! reference the subquery's own tables don't resolve. Because this
//! whole `compile_*` call is inlined at the exact point the subquery
//! expression is evaluated (once per outer row, for a subquery inside
//! a `WHERE`/result-column expression), the outer table's cursor is
//! already correctly positioned on the current row every time this
//! code runs — no coroutine or per-row re-invocation machinery needed.
//!
//! Deliberately out of scope for this pass (see the doc comments on
//! each `compile_*` function below for the exact rejection): `ANY`/
//! `ALL`/`SOME`, and a scalar/`IN`/`EXISTS` subquery-*expression* whose
//! own `FROM` has a `JOIN` (unlike a `FROM`-*subquery*'s own `FROM`
//! having a JOIN, which [`materialize_from_subquery`] (#257) does
//! support). Multi-column `IN` (`(a, b) IN (SELECT ...)`) landed in #251
//! as [`compile_in_subquery_multi`] — it reuses the same ephemeral-index
//! machinery as [`compile_in_subquery`], generalized from a
//! single-register key to a contiguous register range (`Found`/
//! `IdxInsert`'s `P4::Int` key-column-count already supported N > 1).
//!
//! Split (#339, follow-up to #273/#329) into [`from_clause`]
//! (`FROM`-subquery schema resolution/materialization), [`scalar`]
//! (scalar/`EXISTS`/`IN` subquery-expression compilation), [`correlation`]
//! (correlation detection and #306's uncorrelated-subquery hoist), and
//! [`memoize`] (#314's per-probe-value memoization cache for correlated
//! scalar subqueries).

mod correlation;
mod cte;
mod flatten;
mod from_clause;
mod memoize;
mod pushdown;
mod scalar;
mod views;

use crate::parser::ast::Select;

pub use cte::expand_with_clause;
pub use flatten::flatten_from_subqueries;
pub use from_clause::resolve_from_table_schema;
pub use pushdown::push_down_where_predicates;
pub use views::{resolve_views, ExpandViews, ResolvedView};

pub(crate) use correlation::hoist_uncorrelated_where_subqueries;
pub(crate) use from_clause::materialize_from_subquery;
pub(crate) use memoize::{compile_memoized_scalar_subquery, memoize_correlated_where_subqueries};
pub(crate) use scalar::{
    compile_exists, compile_in_subquery, compile_in_subquery_multi, compile_scalar_subquery,
};

/// Identifies a subquery's own `Select` AST node by pointer identity —
/// stable for the lifetime of a single compile pass, since no codegen
/// step clones a `Select`/`Expr` tree once parsing has produced it. Used
/// to key [`Scope::hoisted`] (#306): the same `Select` reference reached
/// once (to hoist/materialize it before the enclosing scan's `Rewind`)
/// and later, per outer row (from `compile_cond`/`compile_value`'s
/// `InSubquery`/`Subquery` dispatch), must resolve to the same map key.
pub(crate) fn select_id(select: &Select) -> usize {
    std::ptr::from_ref(select) as usize
}

/// What a hoisted (materialized-once-before-the-scan) WHERE-clause
/// subquery (#306) precomputed, stashed in [`Scope::hoisted`]: a scalar
/// subquery's already-populated result register, or an uncorrelated
/// `IN`-subquery's already-built ephemeral membership index's cursor.
#[derive(Debug, Clone, Copy)]
pub(crate) enum HoistedSubquery {
    Scalar { reg: i32 },
    In { eph_cursor: i32 },
}

/// A correlated scalar subquery memoized against a single outer column
/// (#314): `cache_cursor` is a table-mode `OpenEphemeral` cursor holding
/// one `(probe_value, result)` row per distinct value of `probe_column`
/// seen so far, opened once before the enclosing scan's `Rewind`. See
/// [`memoize::memoize_correlated_where_subqueries`].
#[derive(Debug, Clone)]
pub(crate) struct MemoizedSubquery {
    pub(crate) cache_cursor: i32,
    pub(crate) probe_column: String,
}
