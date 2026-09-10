// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
use super::*;
/// Where an ORDER BY term's sort key comes from: a raw table column
/// (known schema index, always present in the sorter's row tuple), or
/// a genuine expression that must be computed into its own register
/// and appended to that tuple — its position within the record isn't
/// known until `compile_sorted_scan` actually allocates registers.
#[derive(Debug, Clone)]
pub(super) enum OrderByTarget {
    Column(usize),
    Expr(Expr),
}

pub(super) struct OrderByPlan {
    pub(super) target: OrderByTarget,
    pub(super) descending: bool,
    pub(super) collation: Collation,
    pub(super) nulls_first: bool,
}

pub(super) fn resolve_order_by(
    select: &Select,
    schema: &TableSchema,
) -> Result<Vec<OrderByPlan>, CodegenError> {
    let mut plans = Vec::with_capacity(select.order_by.len());
    for term in &select.order_by {
        let base_expr = strip_collate(&term.expr);
        let target = resolve_order_by_target(base_expr, select, schema)?;
        let descending = term.desc.unwrap_or(false);
        // No NULLS clause defaults to NULLS FIRST for ASC, NULLS LAST for
        // DESC (SQLite's default, matching this compiler's prior
        // behavior); an explicit clause overrides that per direction.
        let nulls_first = term
            .nulls_last
            .map_or(!descending, |nulls_last| !nulls_last);
        let collation = collation_of(&term.expr).unwrap_or_else(|| match &target {
            OrderByTarget::Column(idx) => schema
                .column_collations
                .get(*idx)
                .copied()
                .unwrap_or(Collation::Binary),
            OrderByTarget::Expr(_) => Collation::Binary,
        });
        plans.push(OrderByPlan {
            target,
            descending,
            collation,
            nulls_first,
        });
    }
    Ok(plans)
}

/// Unwraps `expr COLLATE name` (and surrounding parens) down to the
/// expression the ordering is actually keyed on; the collation itself
/// is read separately via `collation_of`.
pub(super) fn strip_collate(expr: &Expr) -> &Expr {
    match &expr.kind {
        ExprKind::Collate { expr: inner, .. } | ExprKind::Paren(inner) => strip_collate(inner),
        _ => expr,
    }
}

/// One result column as seen by ORDER BY ordinal/alias resolution: its
/// full expression (so an ordinal/alias resolving to a computed
/// expression can still become an `OrderByTarget::Expr`) and its `AS`
/// alias, if any. `*`/`table.*` expand against `schema` the same way
/// `result_columns` does, since this compiler is single-table (V2
/// scope).
pub(super) struct OrderByEntry {
    expr: Expr,
    alias: Option<String>,
}

/// A dummy span for expressions synthesized during `*`/`table.*`
/// expansion — not sourced from any actual token, so never used for
/// error reporting.
pub(super) const SYNTHETIC_SPAN: Span = Span {
    line: 0,
    column: 0,
    offset: 0,
    len: 0,
};

/// The compound `SELECT`'s own output column names, in projection
/// order — an alias when the result column has one, else (for a bare
/// column reference) that column's name, else SQLite's positional
/// `columnN` fallback. Used to build the synthetic [`TableSchema`]
/// [`resolve_order_by`] resolves a compound's trailing `ORDER BY`
/// against, since a compound's `ORDER BY` binds to its own result
/// columns (only the first arm's names are visible), never to any
/// arm's underlying table columns.
///
/// Also reused (crate-external, `pub`) by `bin/sqlite-rs/repl.rs` to
/// derive `.headers on` column labels for a single-table `SELECT` —
/// the same "alias, else bare column name, else `columnN`" rule
/// applies there.
pub fn output_column_names(select: &Select, schema: &TableSchema) -> Vec<String> {
    order_by_entries(select, schema)
        .into_iter()
        .enumerate()
        .map(|(i, entry)| {
            entry.alias.unwrap_or_else(|| match &entry.expr.kind {
                ExprKind::Column { name, .. } => name.clone(),
                _ => format!("column{}", i.saturating_add(1)),
            })
        })
        .collect()
}

pub(super) fn order_by_entries(select: &Select, schema: &TableSchema) -> Vec<OrderByEntry> {
    let mut out = Vec::new();
    for col in &select.columns {
        match col {
            ResultColumn::Star | ResultColumn::TableStar { .. } => {
                for name in &schema.columns {
                    out.push(OrderByEntry {
                        expr: Expr {
                            kind: ExprKind::Column {
                                table: None,
                                catalog: None,
                                name: name.clone(),
                            },
                            span: SYNTHETIC_SPAN,
                        },
                        alias: None,
                    });
                }
            }
            ResultColumn::Expr { expr, alias } => out.push(OrderByEntry {
                expr: expr.clone(),
                alias: alias.clone(),
            }),
        }
    }
    out
}

/// Resolves a result-column expression to its `OrderByTarget`: a bare
/// unqualified column becomes a direct schema index (already present
/// in the sorter's row tuple), anything else becomes a computed
/// expression that `compile_sorted_scan` appends to that tuple.
pub(super) fn order_by_target_for_expr(
    expr: &Expr,
    schema: &TableSchema,
) -> Result<OrderByTarget, CodegenError> {
    match &expr.kind {
        ExprKind::Column {
            table: None, name, ..
        } => column_index(schema, name)
            .map(OrderByTarget::Column)
            .ok_or_else(|| CodegenError::UnknownColumn { name: name.clone() }),
        _ => Ok(OrderByTarget::Expr(expr.clone())),
    }
}

pub(super) fn resolve_order_by_target(
    expr: &Expr,
    select: &Select,
    schema: &TableSchema,
) -> Result<OrderByTarget, CodegenError> {
    match &expr.kind {
        ExprKind::Literal(Literal::Integer(n)) => {
            let entries = order_by_entries(select, schema);
            let zero_based = usize::try_from(*n)
                .ok()
                .and_then(|ordinal| ordinal.checked_sub(1));
            let entry = zero_based
                .and_then(|zero_based| entries.get(zero_based))
                .ok_or_else(|| CodegenError::Unsupported {
                    reason: format!(
                        "ORDER BY position {n} is out of range for a {}-column result set",
                        entries.len()
                    ),
                })?;
            order_by_target_for_expr(&entry.expr, schema)
        }
        ExprKind::Column {
            table: None, name, ..
        } => {
            // Result-column aliases take precedence over table columns
            // for ORDER BY (unlike WHERE, where aliases aren't visible
            // at all).
            let entries = order_by_entries(select, schema);
            if let Some(entry) = entries
                .iter()
                .find(|e| e.alias.as_deref() == Some(name.as_str()))
            {
                return order_by_target_for_expr(&entry.expr, schema);
            }
            column_index(schema, name)
                .map(OrderByTarget::Column)
                .ok_or_else(|| CodegenError::UnknownColumn { name: name.clone() })
        }
        _ => order_by_target_for_expr(expr, schema),
    }
}
