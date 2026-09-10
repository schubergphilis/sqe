// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
use super::join_access::{choose_auto_index_probe, choose_join_access, AutoIndexProbe, JoinAccess};
use super::limit_scan::{
    find_covering_index, find_skip_scan_index, is_rowid_reference, top_level_equality_operands,
};
use super::*;
use crate::codegen::subquery::resolve_from_table_schema;
/// One row of `EXPLAIN QUERY PLAN` output (#243) -- SQLite's own EQP
/// shape (`id, parent, notused, detail`), distinct from plain
/// `EXPLAIN`'s per-instruction [`crate::vdbe::explain::ExplainRow`].
/// `detail` reads like the oracle's own EQP (`SCAN ...`/`SEARCH ...
/// USING ...`) but isn't guaranteed byte-identical -- Requirement 10's
/// VM-diff guarantee is plain `EXPLAIN`'s job, not this human-readable
/// summary's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EqpRow {
    /// This row's identifier within the plan.
    pub id: i32,
    /// The `id` of this row's parent in the plan tree (0 for a top-level row).
    pub parent: i32,
    /// Unused column, kept for SQLite's `EXPLAIN QUERY PLAN` shape.
    pub notused: i32,
    /// Human-readable plan step, e.g. `SCAN ...`/`SEARCH ... USING ...`.
    pub detail: String,
}

/// A table binding's `FROM`-clause display name for EQP output:
/// `name AS alias` when aliased, `name` otherwise -- matching how a
/// `Column` reference would need to qualify it.
pub(super) fn eqp_display_name(table_ref: &TableRef) -> String {
    let name = table_ref.name().unwrap_or("(subquery)");
    match &table_ref.alias {
        Some(alias) => format!("{name} AS {alias}"),
        None => name.to_string(),
    }
}

/// The identifier a [`TableBinding`] tracks alongside its `alias` —
/// a subquery-in-FROM (#257) has no catalog name of its own, so this
/// falls back to its (mandatory) alias.
pub(super) fn table_binding_name(table_ref: &TableRef) -> String {
    table_ref
        .name()
        .map(str::to_string)
        .or_else(|| table_ref.alias.clone())
        .unwrap_or_default()
}

/// Builds `EXPLAIN QUERY PLAN`'s output for `select` (#243): one row per
/// `FROM`-clause table, `SCAN` for a full `Rewind`/`Next` scan or
/// `SEARCH ... USING ...` for a `SeekRowid`/`SeekIndexEq` point lookup --
/// reusing [`choose_join_access`] (the join codegen's own decision
/// function) for a join's inner tables, and the same rowid-equality
/// check [`try_compile_rowid_seek`] uses for a single-table `SELECT`'s
/// `WHERE` clause, so the report can never drift from what
/// [`compile_select_joined`]/[`compile_direct_scan`] actually compile.
///
/// #250 note: this reports FROM-clause order, not the RIGHT-JOIN
/// execution reordering `compile_select_joined` may apply internally --
/// `choose_join_access` is still evaluated against each table's
/// FROM-order-preceding siblings, matching what a RIGHT-JOIN-free query
/// actually executes; a query that also has a RIGHT JOIN keeps working
/// via the ordinary (unseeked) fallback below since `on_expr` there
/// won't resolve against these FROM-order-built `prior_bindings`.
///
/// #470's cost-model reordering of a pure `INNER`/`CROSS` chain *is*
/// reflected here (unlike the RIGHT-JOIN case above): `execution_order`
/// mirrors `join_order::plan_join_order`'s decision exactly, rows are
/// emitted in that order, and each join's `ON` clause is associated
/// with the execution level where every table it references is first
/// fully bound (via `join_order::referenced_binding_indices`) rather
/// than assumed adjacent -- matching `compile_select_joined`'s own
/// `LevelCheck` placement for a reordered chain. A level with more than
/// one such check (a multi-table `ON` chain landing on the same level)
/// falls back to reporting a plain `SCAN`, same as
/// `compile_join_level_traverse`'s single-check-only seek optimization.
pub fn explain_query_plan(
    select: &Select,
    schemas: &[TableSchema],
    stats_by_table: &std::collections::HashMap<String, crate::planner::Stats>,
    catalog: &[TableSchema],
) -> Result<Vec<EqpRow>, CodegenError> {
    // #539: a `UNION`/`UNION ALL` compound reports each arm's own plan
    // nested under a synthetic `COMPOUND QUERY` root, matching the
    // oracle's own EQP shape -- `schemas` (already resolved by the
    // caller for `select`'s own `FROM`) only covers the left-most arm;
    // every other arm resolves its own `FROM` against `catalog` here,
    // the same way the subquery recursion below does.
    if !select.compound.is_empty() {
        return explain_compound_query_plan(select, schemas, stats_by_table, catalog);
    }
    let Some(from) = &select.from else {
        return Err(CodegenError::NoFromClause);
    };
    let table_refs: Vec<&TableRef> = std::iter::once(&from.first)
        .chain(from.joins.iter().map(|j| &j.table))
        .collect();
    if schemas.len() != table_refs.len() {
        return Err(CodegenError::Unsupported {
            reason: format!(
                "explain_query_plan needs one schema per FROM table ({} tables, {} schemas \
                 given)",
                table_refs.len(),
                schemas.len()
            ),
        });
    }
    let bindings: Vec<TableBinding> = table_refs
        .iter()
        .zip(schemas.iter())
        .enumerate()
        .map(|(i, (table_ref, schema))| TableBinding {
            alias: table_ref.alias.clone(),
            name: table_binding_name(table_ref),
            schema: schema.clone(),
            cursor: i32::try_from(i).unwrap_or(0),
            forced_null: false,
            stats: stats_by_table
                .get(&schema.name)
                .cloned()
                .unwrap_or_default(),
        })
        .collect();
    let n = bindings.len();

    let reorder = super::join_order::is_reorderable_inner_chain(from).then(|| {
        let on_exprs: Vec<Option<Expr>> = from
            .joins
            .iter()
            .map(|j| match &j.constraint {
                Some(JoinConstraint::On(e)) => Some(e.clone()),
                _ => None,
            })
            .collect();
        let seekable = super::join_order::seekable_tables(schemas, &on_exprs);
        let costs = super::join_order::scan_costs(schemas, stats_by_table, &seekable);
        super::join_order::plan_join_order(&costs)
    });
    let execution_order: Vec<usize> = reorder.clone().unwrap_or_else(|| (0..n).collect());
    let mut pos_of = vec![0usize; n];
    for (pos, &orig) in execution_order.iter().enumerate() {
        if let Some(slot) = pos_of.get_mut(orig) {
            *slot = pos;
        }
    }
    // Only populated when `reorder` fired: `level_joins[level]` lists
    // every join index whose `ON` clause is checkable once execution
    // reaches `level` (i.e. every table it references has
    // `pos_of[..] <= level`) -- mirrors `compile_select_joined_scan`'s
    // reordered-chain `LevelCheck` placement.
    let mut level_joins: Vec<Vec<usize>> = vec![Vec::new(); n];
    if reorder.is_some() {
        for (j, join) in from.joins.iter().enumerate() {
            let right_idx = j.saturating_add(1);
            let on_expr = match &join.constraint {
                Some(JoinConstraint::On(e)) => Some(e),
                _ => None,
            };
            let level = match on_expr {
                Some(e) => super::join_order::referenced_binding_indices(e, &bindings)
                    .into_iter()
                    .chain(std::iter::once(right_idx))
                    .filter_map(|i| pos_of.get(i).copied())
                    .max()
                    .unwrap_or(0),
                None => pos_of.get(right_idx).copied().unwrap_or(0),
            };
            if let Some(slot) = level_joins.get_mut(level) {
                slot.push(j);
            }
        }
    }

    let mut rows = Vec::with_capacity(n);
    let mut next_id: i32 = 0;
    for (level, &orig) in execution_order.iter().enumerate() {
        let Some(&table_ref) = table_refs.get(orig) else {
            continue;
        };
        let Some(binding) = bindings.get(orig) else {
            continue;
        };
        let on_expr = if reorder.is_some() {
            match level_joins.get(level).map(Vec::as_slice) {
                Some([j]) => from.joins.get(*j).and_then(|join| match &join.constraint {
                    Some(JoinConstraint::On(e)) => Some(e),
                    _ => None,
                }),
                _ => None,
            }
        } else {
            level
                .checked_sub(1)
                .and_then(|i| from.joins.get(i))
                .and_then(|j| j.constraint.as_ref())
                .and_then(|c| match c {
                    JoinConstraint::On(e) => Some(e),
                    JoinConstraint::Using(_) => None,
                })
        };
        let prior_bindings: Vec<TableBinding> = if reorder.is_some() {
            bindings
                .iter()
                .enumerate()
                .filter(|&(i, _)| pos_of.get(i).is_some_and(|&p| p < level))
                .map(|(_, b)| b.clone())
                .collect()
        } else {
            bindings.get(..level).unwrap_or(&[]).to_vec()
        };
        let prior_bindings = prior_bindings.as_slice();
        let access = if level == 0 {
            // The outermost table has no `ON` clause to seek against --
            // an equality `WHERE` predicate against its rowid still
            // gets `try_compile_rowid_seek`'s single-table fast path
            // (#137), so report that here too rather than a blanket
            // SCAN.
            select
                .where_clause
                .as_ref()
                .and_then(|where_expr| top_level_equality_operands(where_expr))
                .and_then(|(lhs, rhs)| {
                    if is_rowid_reference(&binding.schema, lhs) {
                        Some(JoinAccess::Rowid(rhs.clone()))
                    } else if is_rowid_reference(&binding.schema, rhs) {
                        Some(JoinAccess::Rowid(lhs.clone()))
                    } else {
                        None
                    }
                })
        } else {
            on_expr.and_then(|e| choose_join_access(binding, e, prior_bindings))
        };
        // #545/#547: when no structural seek exists for this level,
        // report the transient automatic index `choose_auto_index_probe`
        // would build instead of falling through to a blanket `SCAN` --
        // mirrors `joins/level.rs`'s own precedence (tried only once
        // `access` above comes up empty).
        let auto_index_probe = if access.is_none() {
            on_expr.and_then(|e| choose_auto_index_probe(binding, e, prior_bindings))
        } else {
            None
        };
        // #444: a covering-index scan only applies to the outermost
        // table's own `WHERE` clause (like the rowid-seek check above),
        // and only when `access` didn't already find a rowid seek --
        // `find_covering_index` only fires for a non-rowid indexed
        // column, so the two never actually overlap, but checking
        // `access.is_none()` keeps this branch's precedence explicit.
        let covering = if level == 0 && access.is_none() {
            find_covering_index(&binding.schema, select)
        } else {
            None
        };
        // #485: a skip-scan only applies to the outermost table's own
        // `WHERE` clause (like the rowid-seek/covering-index checks
        // above), and only once neither of those already found a
        // cheaper access path -- mirrors `compile_direct_scan`'s
        // dispatch precedence (rowid seek, then covering index, then
        // skip-scan, then plain scan) exactly, so this report can
        // never drift from what actually gets compiled.
        let skip_scan = if level == 0 && access.is_none() && covering.is_none() {
            find_skip_scan_index(&binding.schema, select, &binding.stats)
        } else {
            None
        };
        // #606: BETWEEN/IN/LIKE-prefix range-seek fast paths, checked
        // with the same precedence as `compile_direct_scan`'s dispatch
        // (rowid seek, covering index, then these) — only applies to
        // the outermost table's own `WHERE` clause, like the checks
        // above.
        let range_seek = if level == 0 && access.is_none() && covering.is_none() {
            super::range_scan::find_range_seek_detail(
                &binding.schema,
                select,
                &eqp_display_name(table_ref),
            )
        } else {
            None
        };
        let detail = if let Some(range_detail) = range_seek {
            range_detail
        } else {
            match (
                access,
                covering
                    .as_ref()
                    .and_then(|m| binding.schema.indexes.get(m.index_position)),
                skip_scan
                    .as_ref()
                    .and_then(|m| binding.schema.indexes.get(m.index_position))
                    .zip(skip_scan.as_ref().map(|m| m.column_position)),
            ) {
                (_, Some(index), _) => format!(
                    "SEARCH {} USING COVERING INDEX {} ({}=?)",
                    eqp_display_name(table_ref),
                    index.name,
                    index
                        .columns
                        .first()
                        .map_or_else(String::new, |c| c.name.clone())
                ),
                (None, None, Some((index, column_position))) => {
                    // Oracle sqlite3's own skip-scan EQP text, confirmed
                    // empirically (sqlite3 3.51.0): `SEARCH t USING INDEX
                    // idx (ANY(category) AND price=?)` -- one `ANY(col)`
                    // per unconstrained leading column, then `col=?` for
                    // the actually-probed column.
                    let parts: Vec<String> = index
                        .columns
                        .get(..column_position)
                        .unwrap_or_default()
                        .iter()
                        .map(|c| format!("ANY({})", c.name))
                        .chain(std::iter::once(format!(
                            "{}=?",
                            index
                                .columns
                                .get(column_position)
                                .map_or_else(String::new, |c| c.name.clone())
                        )))
                        .collect();
                    format!(
                        "SEARCH {} USING INDEX {} ({})",
                        eqp_display_name(table_ref),
                        index.name,
                        parts.join(" AND ")
                    )
                }
                // #545/#547: this level builds a transient automatic index
                // rather than falling back to a plain scan -- real sqlite3's
                // own wording for the equivalent case (confirmed empirically,
                // sqlite3 3.51.0): `SEARCH t USING AUTOMATIC COVERING INDEX
                // (col=?)`, no index name (it has none).
                (None, None, None) => match &auto_index_probe {
                    Some(AutoIndexProbe { key_column, .. }) => format!(
                        "SEARCH {} USING AUTOMATIC COVERING INDEX ({}=?)",
                        eqp_display_name(table_ref),
                        binding
                            .schema
                            .columns
                            .get(*key_column)
                            .map_or_else(String::new, |c| c.clone())
                    ),
                    None => format!("SCAN {}", eqp_display_name(table_ref)),
                },
                (Some(JoinAccess::Rowid(_)), None, _) => format!(
                    "SEARCH {} USING INTEGER PRIMARY KEY (rowid=?)",
                    eqp_display_name(table_ref)
                ),
                (Some(JoinAccess::UniqueIndex { index, .. }), None, _) => format!(
                    "SEARCH {} USING INDEX {} ({}=?)",
                    eqp_display_name(table_ref),
                    index.name,
                    index
                        .columns
                        .first()
                        .map_or_else(String::new, |c| c.name.clone())
                ),
            }
        };
        let row_id = next_id;
        next_id = next_id.saturating_add(1);
        rows.push(EqpRow {
            id: row_id,
            parent: 0,
            notused: 0,
            detail,
        });

        // #532: a materialized `FROM`-subquery/view has its own inner
        // scan (`materialize_from_subquery`'s `compile_select_scan` --
        // possibly now an index seek, once a WHERE conjunct got pushed
        // into `table_ref`'s own `where_clause`) that this row's plain
        // "SCAN ..."/"SEARCH ..." text can't describe on its own, since
        // it describes the *outer* query's access to the materialized
        // result, not what filled it. Recurse into the subquery's own
        // plan and nest its rows underneath this one, offsetting ids so
        // they stay unique across the whole (possibly further-nested)
        // tree.
        if let TableRefKind::Subquery(inner) = &table_ref.kind {
            if let Some(inner_from) = &inner.from {
                let inner_table_refs: Vec<&TableRef> = std::iter::once(&inner_from.first)
                    .chain(inner_from.joins.iter().map(|j| &j.table))
                    .collect();
                let inner_schemas: Result<Vec<TableSchema>, CodegenError> = inner_table_refs
                    .iter()
                    .map(|table_ref| resolve_from_table_schema(table_ref, catalog))
                    .collect();
                if let Ok(inner_schemas) = inner_schemas {
                    if let Ok(child_rows) =
                        explain_query_plan(inner, &inner_schemas, stats_by_table, catalog)
                    {
                        let offset = next_id;
                        for mut child in child_rows {
                            let was_top_level = child.parent == 0;
                            child.id = child.id.saturating_add(offset);
                            child.parent = if was_top_level {
                                row_id
                            } else {
                                child.parent.saturating_add(offset)
                            };
                            next_id = next_id.max(child.id.saturating_add(1));
                            rows.push(child);
                        }
                    }
                }
            }
        }
    }
    // #654: a single-table `GROUP BY` that can't walk an existing index
    // in key order needs a `Sorter` (`compile_grouped_scan`'s own doc
    // comment) to group rows -- reusing
    // `aggregate::group_by_index_ordering`, the exact eligibility check
    // `compile_select_scan`'s dispatch itself gates
    // `try_compile_index_ordered_group_by` on, so this report can never
    // drift from which strategy actually got compiled. Real sqlite3's
    // own wording for the Sorter-backed case (confirmed empirically,
    // sqlite3 3.53.4): `USE TEMP B-TREE FOR GROUP BY`. Joined `GROUP BY`
    // (`compile_joined_grouped_scan`) isn't covered here -- out of scope
    // for #654, which only found this gap via a single-table scenario.
    if from.joins.is_empty() && !select.group_by.is_empty() {
        if let Some(binding) = bindings.first() {
            let index_ordered =
                super::aggregate::group_by_index_ordering(select, &binding.schema, false)?
                    .is_some();
            if !index_ordered {
                rows.push(EqpRow {
                    id: next_id,
                    parent: 0,
                    notused: 0,
                    detail: "USE TEMP B-TREE FOR GROUP BY".to_string(),
                });
            }
        }
    }
    Ok(rows)
}

/// A `CompoundSelect` arm carries the same core fields as a `Select`
/// (columns/from/where/group-by/having) but none of the whole-statement
/// ones (`with_clause`/further `compound`/`order_by`/`limit`) -- this
/// rebuilds a plain `Select` from an arm so [`explain_query_plan`] can
/// analyze it exactly like a top-level `SELECT`.
fn compound_arm_as_select(arm: &CompoundSelect) -> Select {
    Select {
        with_clause: None,
        distinct: arm.distinct,
        columns: arm.columns.clone(),
        from: arm.from.clone(),
        where_clause: arm.where_clause.clone(),
        group_by: arm.group_by.clone(),
        having: arm.having.clone(),
        compound: Vec::new(),
        order_by: Vec::new(),
        limit: None,
        span: arm.span,
    }
}

/// Oracle sqlite3's own compound-operator EQP text, confirmed
/// empirically (sqlite3 3.51.0): plain `UNION` dedups via an ephemeral
/// index (matching #377/#378's actual codegen), so its EQP text calls
/// that out; `UNION ALL` keeps every row and needs no such step.
fn compound_op_label(op: CompoundOp) -> &'static str {
    match op {
        CompoundOp::Union => "UNION USING TEMP B-TREE",
        CompoundOp::UnionAll => "UNION ALL",
    }
}

/// #539: `explain_query_plan`'s compound-select branch -- one
/// `COMPOUND QUERY` root row, a `LEFT-MOST SUBQUERY` child holding
/// `select`'s own (non-compound) plan, then one `UNION`/`UNION ALL`
/// child per arm holding that arm's plan. Every nested tree's ids are
/// offset so they stay unique across the whole result, the same
/// offsetting scheme the `FROM`-subquery recursion above uses.
fn explain_compound_query_plan(
    select: &Select,
    schemas: &[TableSchema],
    stats_by_table: &std::collections::HashMap<String, crate::planner::Stats>,
    catalog: &[TableSchema],
) -> Result<Vec<EqpRow>, CodegenError> {
    let mut rows = Vec::new();
    let mut next_id: i32 = 0;

    let compound_id = next_id;
    next_id = next_id.saturating_add(1);
    rows.push(EqpRow {
        id: compound_id,
        parent: 0,
        notused: 0,
        detail: "COMPOUND QUERY".to_string(),
    });

    let leftmost_id = next_id;
    next_id = next_id.saturating_add(1);
    rows.push(EqpRow {
        id: leftmost_id,
        parent: compound_id,
        notused: 0,
        detail: "LEFT-MOST SUBQUERY".to_string(),
    });
    let leftmost = Select {
        compound: Vec::new(),
        ..select.clone()
    };
    let leftmost_rows = explain_query_plan(&leftmost, schemas, stats_by_table, catalog)?;
    graft_child_plan(&mut rows, &mut next_id, leftmost_id, leftmost_rows);

    for arm in &select.compound {
        let op_id = next_id;
        next_id = next_id.saturating_add(1);
        rows.push(EqpRow {
            id: op_id,
            parent: compound_id,
            notused: 0,
            detail: compound_op_label(arm.op).to_string(),
        });

        let arm_select = compound_arm_as_select(arm);
        let arm_schemas: Vec<TableSchema> = match &arm.from {
            Some(arm_from) => std::iter::once(&arm_from.first)
                .chain(arm_from.joins.iter().map(|j| &j.table))
                .map(|table_ref| resolve_from_table_schema(table_ref, catalog))
                .collect::<Result<Vec<_>, CodegenError>>()?,
            None => Vec::new(),
        };
        let arm_rows = explain_query_plan(&arm_select, &arm_schemas, stats_by_table, catalog)?;
        graft_child_plan(&mut rows, &mut next_id, op_id, arm_rows);
    }

    Ok(rows)
}

/// Appends `child_rows` (a nested `explain_query_plan` result) into
/// `rows` under `parent_id`, offsetting every id by `*next_id` so ids
/// stay unique across the whole tree -- the same offsetting scheme the
/// `FROM`-subquery recursion in [`explain_query_plan`] uses.
fn graft_child_plan(
    rows: &mut Vec<EqpRow>,
    next_id: &mut i32,
    parent_id: i32,
    child_rows: Vec<EqpRow>,
) {
    let offset = *next_id;
    for mut row in child_rows {
        let was_top_level = row.parent == 0;
        row.id = row.id.saturating_add(offset);
        row.parent = if was_top_level {
            parent_id
        } else {
            row.parent.saturating_add(offset)
        };
        *next_id = (*next_id).max(row.id.saturating_add(1));
        rows.push(row);
    }
}
