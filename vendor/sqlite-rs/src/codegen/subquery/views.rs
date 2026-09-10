// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! `CREATE VIEW` query expansion (#380) — the catalog counterpart to
//! [`super::cte::expand_with_clause`]: every `FROM`/`JOIN` table
//! reference naming a catalog view becomes a `TableRefKind::Subquery`
//! wrapping that view's stored `Select`, reusing exactly the same
//! `TableRefKind::Subquery` materialization path (#257) CTEs already
//! ride on. Unlike CTEs (declared inline, resolved in one pass in
//! declaration order), every view is already fully defined in the
//! catalog before a query ever runs, so a view referencing another view
//! (nested views) is resolved by recursing into the substituted
//! subquery's own `FROM`/`JOIN` clauses with the same view list. Since
//! `CREATE VIEW` never checks for cycles at DDL time, a view can
//! reference itself directly or transitively (through other views); the
//! stack of view names currently being expanded is tracked so such a
//! cycle is rejected with [`CodegenError::CircularView`] (matching stock
//! SQLite's own "view X is circularly defined" wording) instead of
//! recursing forever.

use std::borrow::Cow;

use crate::parser::ast::{Select, TableRef, TableRefKind};
use crate::parser::error::ParseOutcome;

use crate::codegen::select::CodegenError;

use super::cte::{apply_column_aliases, expand_with_clause};

/// A view's catalog entry, pre-parsed once per query by the caller
/// (`bin/sqlite-rs/query.rs`) from `schema::ViewSchema::sql`.
pub struct ResolvedView {
    /// The view's name, as it appears in `sqlite_master`.
    pub name: String,
    /// Optional explicit column name list from `CREATE VIEW name(cols)`.
    pub columns: Option<Vec<String>>,
    /// The view's underlying, already-parsed `SELECT`.
    pub query: Box<Select>,
}

/// Parses every `schema::ViewSchema` into a [`ResolvedView`], silently
/// dropping any view whose stored `sql` no longer parses (should not
/// happen in practice — `sqlite_master.sql` is only ever written by
/// [`crate::codegen::compile_create_view`] itself — but this module
/// follows the same graceful-degradation convention as
/// `schema::ddl_reader`'s unparseable-DDL handling rather than turning a
/// single bad row into a hard failure for every other query).
pub fn resolve_views(views: &[crate::schema::ViewSchema]) -> Vec<ResolvedView> {
    views
        .iter()
        .filter_map(|v| match crate::parser::parse_create_view(&v.sql) {
            ParseOutcome::Accepted(create) => Some(ResolvedView {
                name: create.name,
                columns: create.columns,
                query: create.query,
            }),
            _ => None,
        })
        .collect()
}

/// Rewrites away every catalog-view reference in `select`'s `FROM`/
/// `JOIN` clauses (main query and each `UNION`/`UNION ALL` arm),
/// recursively. A `Select` that references no view (including no view
/// reachable through a `TableRefKind::Subquery` a prior CTE rewrite
/// produced) is returned as `Cow::Borrowed` rather than cloned — the
/// common case for a query with no view in scope at all (#590 item 6).
/// Fails with [`CodegenError::CircularView`] if a view directly or
/// transitively references itself.
///
/// A method (rather than a free function taking `select: &Select,
/// views: &[ResolvedView]`) specifically so the returned `Cow<'_,
/// Select>`'s lifetime elides to the receiver's alone — the qualified
/// subset (`make check-mvl-limit`) denies explicit lifetime parameters, and a
/// free function with two independently-lifetimed reference parameters
/// has no elided borrow to tie an elided `Cow<'_, _>` to.
pub trait ExpandViews {
    /// See [`ExpandViews`]'s trait-level doc.
    fn expand_views(&self, views: &[ResolvedView]) -> Result<Cow<'_, Select>, CodegenError>;
}

impl ExpandViews for Select {
    fn expand_views(&self, views: &[ResolvedView]) -> Result<Cow<'_, Select>, CodegenError> {
        if views.is_empty() || !select_references_any_view(self, views) {
            return Ok(Cow::Borrowed(self));
        }
        let mut out = self.clone();
        let mut stack = Vec::new();
        expand_views_in_select(&mut out, views, &mut stack)?;
        Ok(Cow::Owned(out))
    }
}

/// Read-only check mirroring [`expand_views_in_select`]'s traversal
/// exactly (main `FROM`/`JOIN`, each compound arm, recursing into any
/// `TableRefKind::Subquery`) so the "nothing to rewrite" fast path in
/// [`expand_views`] can never disagree with what the rewrite itself
/// would have found.
fn select_references_any_view(select: &Select, views: &[ResolvedView]) -> bool {
    let from_has_view = |from: &crate::parser::ast::FromClause| {
        table_ref_references_view(&from.first, views)
            || from
                .joins
                .iter()
                .any(|j| table_ref_references_view(&j.table, views))
    };
    if select.from.as_ref().is_some_and(from_has_view) {
        return true;
    }
    select
        .compound
        .iter()
        .any(|arm| arm.from.as_ref().is_some_and(from_has_view))
}

fn table_ref_references_view(table_ref: &TableRef, views: &[ResolvedView]) -> bool {
    match &table_ref.kind {
        TableRefKind::Name(name) => views.iter().any(|v| v.name.eq_ignore_ascii_case(name)),
        TableRefKind::Subquery(inner) => select_references_any_view(inner, views),
    }
}

fn expand_views_in_select(
    select: &mut Select,
    views: &[ResolvedView],
    stack: &mut Vec<String>,
) -> Result<(), CodegenError> {
    if let Some(from) = &mut select.from {
        expand_table_ref(&mut from.first, views, stack)?;
        for join in &mut from.joins {
            expand_table_ref(&mut join.table, views, stack)?;
        }
    }
    for arm in &mut select.compound {
        if let Some(from) = &mut arm.from {
            expand_table_ref(&mut from.first, views, stack)?;
            for join in &mut from.joins {
                expand_table_ref(&mut join.table, views, stack)?;
            }
        }
    }
    Ok(())
}

fn expand_table_ref(
    table_ref: &mut TableRef,
    views: &[ResolvedView],
    stack: &mut Vec<String>,
) -> Result<(), CodegenError> {
    match &mut table_ref.kind {
        TableRefKind::Name(name) => {
            let Some(view) = views.iter().find(|v| v.name.eq_ignore_ascii_case(name)) else {
                return Ok(());
            };
            if stack
                .iter()
                .any(|seen| seen.eq_ignore_ascii_case(&view.name))
            {
                return Err(CodegenError::CircularView {
                    name: view.name.clone(),
                });
            }
            // A view's own stored body may carry its own `WITH` clause
            // (`CREATE VIEW v AS WITH cte AS (...) SELECT * FROM cte`) —
            // that never runs through `expand_with_clause` otherwise,
            // since only the outermost query gets that pass at the top
            // of `compile_select_program`. Expanding it here, before
            // recursing for nested views, mirrors the ordering already
            // used at the top level (CTEs first, then views).
            let mut query = expand_with_clause(&view.query).into_owned();
            if let Some(columns) = &view.columns {
                apply_column_aliases(&mut query, columns);
            }
            stack.push(view.name.clone());
            let result = expand_views_in_select(&mut query, views, stack);
            stack.pop();
            result?;
            // Same alias-defaulting rule as CTE substitution (#376): a
            // bare `FROM view_name` (no explicit alias) needs the
            // subquery's alias set to the view's own name, since a
            // `TableRefKind::Subquery`'s alias is mandatory to the rest
            // of codegen (`resolve_from_table_schema`'s doc comment).
            let alias = table_ref.alias.clone().or_else(|| Some(view.name.clone()));
            table_ref.kind = TableRefKind::Subquery(Box::new(query));
            table_ref.alias = alias;
            Ok(())
        }
        TableRefKind::Subquery(inner) => expand_views_in_select(inner, views, stack),
    }
}
