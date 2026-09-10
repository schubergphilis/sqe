// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! Compiling an arbitrary SQL string, whatever kind of statement it is.
//!
//! [`compile_statement`](crate::codegen::compile_statement) covers write
//! and DDL statements only — hand it a `SELECT` and it answers
//! `DispatchError::Unrecognized("SELECT")`. A `SELECT` needs its `FROM`
//! tables resolved, its CTEs and views expanded, and `sqlite_stat1`
//! statistics loaded before it can be compiled at all, and the pipeline
//! that does that lived in `src/bin/sqlite-rs/query.rs` — inside the
//! executable, where no library consumer could reach it.
//!
//! That split is a CLI implementation detail, not something a consumer
//! should have to know. Stock SQLite has exactly one
//! `sqlite3_prepare_v2()`: you hand it any statement and get a handle
//! back. Spec 013's `Connection::prepare` has to behave the same way, so
//! the `SELECT` half moves here where both the CLI and the embedding API
//! can call it (013/Req 3, #695).
//!
//! Errors are [`CodegenError`]/[`PrepareError`] rather than the `String`
//! the CLI copy produced. Flattening a structured error into a message is
//! fine when the next step is printing it to a terminal; a library
//! consumer needs to match on what went wrong.

use std::collections::HashMap;
use std::fmt;

use crate::codegen::{
    compile_select_compound, compile_select_joined, compile_select_with_catalog,
    compile_select_with_catalog_and_stats, expand_with_clause, explain_query_plan,
    flatten_from_subqueries, output_column_names, push_down_where_predicates,
    resolve_from_table_schema, resolve_views, CodegenError, EqpRow, ExpandViews,
};
use crate::parser::ast::{Select, TableRef};
use crate::planner::Stats;
use crate::schema::{TableSchema, ViewSchema};
use crate::vdbe::Program;

/// What [`compile_select_program`] produced: either `EXPLAIN QUERY PLAN`'s rows
/// (nothing further to compile — there is no bytecode to run) or an
/// ordinary compiled [`Program`].
#[derive(Debug)]
pub enum SelectOutcome {
    /// `EXPLAIN QUERY PLAN` output rows.
    Eqp(Vec<EqpRow>),
    /// A compiled program, ready to execute.
    Program(Program),
}

/// Why a `SELECT` could not be compiled.
///
/// Distinct from [`CodegenError`] only for the cases that are about the
/// *request* rather than the SQL: asking for `EXPLAIN QUERY PLAN` on a
/// statement that has no `FROM` clause to plan.
#[derive(Debug, PartialEq, Eq)]
pub enum PrepareError {
    /// `EXPLAIN QUERY PLAN` was requested for a FROM-less `SELECT`.
    ///
    /// Not a `CodegenError::NoFromClause`: a FROM-less `SELECT 1` is
    /// perfectly compilable, it just has no access path to explain.
    EqpWithoutFrom,
    /// The statement itself could not be compiled.
    Codegen(CodegenError),
}

impl fmt::Display for PrepareError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PrepareError::EqpWithoutFrom => {
                write!(f, "EXPLAIN QUERY PLAN requires a FROM clause")
            }
            PrepareError::Codegen(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for PrepareError {}

impl From<CodegenError> for PrepareError {
    fn from(e: CodegenError) -> Self {
        PrepareError::Codegen(e)
    }
}

/// Resolves every table a `SELECT` touches and compiles it — FROM-less
/// (#260), single-table, joined (#237), or compound (#240), whichever
/// its shape calls for.
///
/// `select`/`eqp_mode` come from `parse_select`/`parse_explain`; parsing
/// is the caller's. Lifted verbatim from the CLI's own
/// `compile_select_program` (#695), with `String` errors replaced by
/// [`PrepareError`] — the CLI and the embedding API need exactly this
/// pipeline, just against a different `PageSource`.
pub fn compile_select_program(
    select: &Select,
    eqp_mode: bool,
    schemas: &[TableSchema],
    views: &[ViewSchema],
    stats_by_table: &HashMap<String, Stats>,
) -> Result<SelectOutcome, PrepareError> {
    // #376: a `WITH` clause is rewritten away before any table
    // resolution happens — every CTE reference in `FROM`/`JOIN` becomes
    // a `TableRefKind::Subquery` wrapping that CTE's own query, so the
    // rest of this pipeline (and #257's subquery-in-FROM codegen) needs
    // no CTE-specific handling at all.
    let cte_expanded = expand_with_clause(select);
    // #380: every catalog-view reference is rewritten away next, the
    // same shape as the CTE rewrite. Runs *after* it so it also reaches
    // into any `TableRefKind::Subquery` the CTE rewrite just produced.
    let resolved_views = resolve_views(views);
    let expanded = cte_expanded.expand_views(&resolved_views)?;
    // The passes below need `&mut Select`, so this is where the deferred
    // clone (if any — `Cow` was `Borrowed` for the common
    // no-CTE/no-view case) finally happens, at most once total.
    let mut expanded = expanded.into_owned();
    // #566: flatten a simple FROM-subquery/view/CTE into the enclosing
    // query first — eliminating it outright makes any base-table index
    // it hides visible to the planner, which a predicate push-down
    // cannot do.
    flatten_from_subqueries(&mut expanded);
    // #532: push safely-movable outer WHERE conjuncts into whatever
    // flattening did not eliminate.
    push_down_where_predicates(&mut expanded);
    let select = &expanded;

    let resolve_table = |table_ref: &TableRef| -> Result<TableSchema, CodegenError> {
        resolve_from_table_schema(table_ref, schemas)
    };

    let Some(from) = &select.from else {
        if eqp_mode {
            return Err(PrepareError::EqpWithoutFrom);
        }
        let program = compile_select_with_catalog(select, &from_less_schema(), &[])?;
        return Ok(SelectOutcome::Program(program));
    };

    let schema = resolve_table(&from.first)?;

    if eqp_mode {
        let mut joined_schemas = vec![schema];
        for join in &from.joins {
            joined_schemas.push(resolve_table(&join.table)?);
        }
        let rows = explain_query_plan(select, &joined_schemas, stats_by_table, schemas)?;
        return Ok(SelectOutcome::Eqp(rows));
    }

    let program = if !select.compound.is_empty() {
        let mut arm_schemas = Vec::with_capacity(select.compound.len());
        for arm in &select.compound {
            let Some(arm_from) = &arm.from else {
                return Err(CodegenError::NoFromClause.into());
            };
            arm_schemas.push(resolve_table(&arm_from.first)?);
        }
        compile_select_compound(select, &schema, &arm_schemas, schemas)?
    } else if from.joins.is_empty() {
        let stats = stats_by_table
            .get(&schema.name)
            .cloned()
            .unwrap_or_default();
        compile_select_with_catalog_and_stats(select, &schema, schemas, &stats)?
    } else {
        let mut joined_schemas = vec![schema];
        for join in &from.joins {
            joined_schemas.push(resolve_table(&join.table)?);
        }
        compile_select_joined(select, &joined_schemas, schemas, stats_by_table)?
    };
    Ok(SelectOutcome::Program(program))
}

/// The result column names a `SELECT` produces, for access by name.
///
/// Falls back to `column1`, `column2`, ... for shapes whose names this
/// cannot derive (joins and compounds), which is what the CLI already
/// printed as headers for them. Lifted from the CLI's `derive_headers`
/// (#695) so `Row::get_by_name` and the CLI agree by construction rather
/// than by coincidence.
pub fn result_column_names(select: &Select, schemas: &[TableSchema]) -> Vec<String> {
    let single_table = select.compound.is_empty()
        && select
            .from
            .as_ref()
            .is_some_and(|from| from.joins.is_empty());
    if single_table {
        if let Some(from) = &select.from {
            if let Ok(schema) = resolve_from_table_schema(&from.first, schemas) {
                return output_column_names(select, &schema);
            }
        }
    }
    let count = select.columns.len().max(1);
    (1..=count).map(|i| format!("column{i}")).collect()
}

/// The placeholder schema a FROM-less `SELECT` compiles against (#260).
///
/// Private on purpose: the CLI inlined this literal, and a public
/// `TableSchema::none()` would be new API surface this lift does not
/// need.
fn from_less_schema() -> TableSchema {
    TableSchema {
        name: String::new(),
        root_page: 0,
        columns: vec![],
        column_types: vec![],
        column_collations: vec![],
        without_rowid: false,
        strict: false,
        is_virtual: false,
        sql: String::new(),
        indexes: vec![],
        rowid_alias: None,
        unresolved_autoindex: false,
    }
}
