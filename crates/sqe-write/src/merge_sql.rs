//! MERGE INTO clause classification and merge-SELECT generation.
//!
//! The copy-on-write MERGE path compiles the statement into one SELECT over a
//! FULL OUTER JOIN of the target and source scratch tables. Every row falls
//! into exactly one class:
//!
//! - matched: both sides present, governed by `WHEN MATCHED` clauses
//! - source-only: no target row, governed by `WHEN NOT MATCHED [BY TARGET]`
//! - target-only: no source row, governed by `WHEN NOT MATCHED BY SOURCE`
//!
//! Clauses are honored in statement order with first-match-wins semantics
//! (SQL standard): within a class, the first clause whose predicate passes
//! decides the row's fate; a row no clause claims passes through unchanged
//! (matched / target-only) or is not inserted (source-only).
//!
//! Row removal (MATCHED DELETE, BY SOURCE DELETE, unclaimed source-only rows)
//! is expressed through a boolean [`MERGE_KEEP_COLUMN`] computed alongside the
//! data columns and filtered in an outer `WHERE`, so deleted rows never reach
//! the write sink. This replaces the earlier all-NULL marker rows that had to
//! be filtered out of the output stream.
//!
//! dbt SCD2 snapshots are the motivating shape: multiple predicated
//! `WHEN MATCHED AND <cond>` clauses plus a predicated
//! `WHEN NOT MATCHED AND <cond> THEN INSERT`.

use sqe_core::SqeError;
use sqe_policy::write_predicates::WritePolicyPredicates;
use sqlparser::ast::{
    Assignment, MergeAction, MergeClause, MergeClauseKind, MergeInsertKind, ObjectName,
};

/// Name of the synthetic boolean column that marks rows surviving the MERGE.
/// It exists only inside the merge SELECT; the outer projection drops it.
pub(crate) const MERGE_KEEP_COLUMN: &str = "__sqe_merge_keep";

/// Synthetic boolean column marking rows some MERGE clause claimed; only in
/// the WITH CHECK count query.
const MERGE_CLAIMED_COLUMN: &str = "__sqe_merge_claimed";

/// Naming context shared by classification and SELECT generation. The
/// `*_ref` names are the per-invocation scratch table names the target and
/// source relations are registered under; `t_alias`/`s_alias` are the
/// aliases the user's SQL referenced them by.
pub(crate) struct MergeNames<'a> {
    pub target_ref: &'a str,
    pub source_ref: &'a str,
    pub t_alias: &'a str,
    pub s_alias: &'a str,
    pub target_columns: &'a [String],
    pub source_columns: &'a [String],
    /// The caller's row filter and column masks on the TARGET table
    /// (architecture review 202). Applied the way PostgreSQL RLS applies to
    /// MERGE: a target row the caller cannot SELECT is never matched and
    /// never touched by a NOT MATCHED BY SOURCE clause; it passes through the
    /// rewrite unchanged. A masked column is compared and read through its
    /// mask in ON, clause predicates and SET expressions (bare or qualified),
    /// while the engine's own passthrough keeps the raw value so unchanged
    /// rows are rewritten byte-for-byte.
    pub target_policy: &'a WritePolicyPredicates,
    /// SQL for the `write_default` of each target column that has one, which
    /// a NOT MATCHED INSERT writes when it does not supply the column (#828).
    pub column_defaults: &'a std::collections::HashMap<String, String>,
}

/// Name of the synthetic boolean column that says whether the caller's row
/// filter admits a target row. Exists only inside the merge SQL.
pub(crate) const MERGE_ADMIT_COLUMN: &str = "__sqe_merge_admit";

/// Column alias carrying the RAW value of a masked or row-filter-guarded
/// target column inside the merge SQL, for the engine's passthrough only. It
/// embeds the per-invocation scratch ref (a fresh UUID), so user SQL cannot
/// name it; the column's own name carries the value the caller sees.
fn raw_alias(names: &MergeNames<'_>, col: &str) -> String {
    format!("__sqe_raw_{}_{col}", names.target_ref)
}

/// Whether the caller reads target column `col` through the policy (a mask
/// or the row-filter guard), so its own name does not carry the stored value.
fn read_via_policy(names: &MergeNames<'_>, col: &str) -> bool {
    names.target_policy.row_filter_sql.is_some()
        || names.target_policy.column_mask_sqls.contains_key(col)
}

/// The engine's reference to the stored value of target column `col`: the
/// raw alias for a column read through the policy, the column itself
/// otherwise.
pub(crate) fn raw_target_col(names: &MergeNames<'_>, col: &str) -> String {
    if read_via_policy(names, col) {
        format!("{}.\"{}\"", names.target_ref, raw_alias(names, col))
    } else {
        format!("{}.\"{col}\"", names.target_ref)
    }
}

/// The target relation as the merge SQL should read it: the bare scratch
/// table when the caller has no policy on the target, otherwise a relation
/// that projects each masked column's mask under the column's own name (so
/// any user reference, bare or qualified, reads it masked), its raw value
/// under [`raw_alias`], and the `MERGE_ADMIT_COLUMN` row-filter verdict.
/// Every policy expression is evaluated inside this single-table relation,
/// so the unqualified column names the rewriter produced resolve without
/// ambiguity.
///
/// With column masks the relation is the view [`register_target_policy_view`]
/// registers: mask SQL can call the internal keyed HASH function, which the
/// user's session (where the merge SQL, with the user's ON / SET / clause
/// predicates, is planned) deliberately cannot resolve. If the view was not
/// registered the merge SQL fails to plan; there is no inline fallback. A
/// row filter alone stays inline.
pub(crate) fn target_relation(names: &MergeNames<'_>, qualified_target_ref: &str) -> String {
    let policy = names.target_policy;
    if policy.is_empty() {
        return qualified_target_ref.to_string();
    }
    if let Some(view) = target_policy_view(names) {
        return view;
    }
    format!("({})", target_policy_select(names, qualified_target_ref))
}

/// Name of the masked target view for this MERGE invocation, or `None` when
/// the target has no column masks. It embeds the per-invocation scratch ref,
/// and its `__sqe_` prefix in the `datafusion` catalog marks it as engine
/// scratch for the DML policy pass.
pub(crate) fn target_policy_view(names: &MergeNames<'_>) -> Option<String> {
    (!names.target_policy.column_mask_sqls.is_empty())
        .then(|| format!("datafusion.public.__sqe_mpol_{}", names.target_ref))
}

/// Register the masked target view [`target_relation`] references, planned
/// in the separate mask context ([`crate::policy_view`]). A no-op without
/// column masks. Call it after the target scratch table is registered, and
/// again whenever that registration is replaced: the view captures the
/// table it was planned against. The caller deregisters
/// [`target_policy_view`].
pub(crate) async fn register_target_policy_view(
    ctx: &datafusion::prelude::SessionContext,
    names: &MergeNames<'_>,
    qualified_target_ref: &str,
) -> sqe_core::Result<()> {
    let Some(view) = target_policy_view(names) else {
        return Ok(());
    };
    crate::policy_view::register_policy_view(
        ctx,
        &view,
        &target_policy_select(names, qualified_target_ref),
        names.target_policy,
    )
    .await
}

/// The policy SELECT over the target scratch table behind [`target_relation`].
///
/// With a row filter every column is read through the guard
/// `CASE WHEN <row filter> THEN <col> END`, so a hidden target row is all
/// NULL to the user's ON, clause predicates and SET expressions. The
/// optimizer may evaluate those on every row (a predicate pushed below the
/// join, the right side of an AND over the whole batch), and a fallible one,
/// say `1 / (t.id - 2)`, would otherwise fail on a hidden row and reveal its
/// values. The admit verdict is computed on the stored values, TRUE-only.
fn target_policy_select(names: &MergeNames<'_>, qualified_target_ref: &str) -> String {
    let policy = names.target_policy;
    let row_filter = policy.row_filter_sql.as_deref();
    let mut projections = Vec::with_capacity(2 * names.target_columns.len() + 1);
    for col in names.target_columns {
        let mask = policy.column_mask_sqls.get(col);
        let seen = match mask {
            Some(mask_sql) => format!("({mask_sql})"),
            None => format!("\"{col}\""),
        };
        match row_filter {
            Some(rf) => projections.push(format!("CASE WHEN ({rf}) THEN {seen} END AS \"{col}\"")),
            None if mask.is_some() => projections.push(format!("{seen} AS \"{col}\"")),
            None => projections.push(seen),
        }
        if read_via_policy(names, col) {
            projections.push(format!("\"{col}\" AS \"{}\"", raw_alias(names, col)));
        }
    }
    if let Some(rf) = row_filter {
        projections.push(format!("(({rf}) IS TRUE) AS \"{MERGE_ADMIT_COLUMN}\""));
    }
    format!(
        "SELECT {} FROM {qualified_target_ref}",
        projections.join(", ")
    )
}

/// `TRUE`-only guard on the target's admit column, or `None` without a row
/// filter. ANDed into the ON condition and the target-only row class.
pub(crate) fn admit_guard(names: &MergeNames<'_>) -> Option<String> {
    names
        .target_policy
        .row_filter_sql
        .as_ref()
        .map(|_| format!("{}.\"{MERGE_ADMIT_COLUMN}\"", names.target_ref))
}

/// The ON condition with the caller's row filter folded in, so a hidden
/// target row is never matched.
pub(crate) fn guarded_on(names: &MergeNames<'_>, on_rewritten: &str) -> String {
    match admit_guard(names) {
        Some(guard) => format!("({on_rewritten}) AND {guard}"),
        None => on_rewritten.to_string(),
    }
}

/// A MERGE clause action after validation against its clause kind.
pub(crate) enum MergeOp<'a> {
    Update(&'a [Assignment]),
    Delete,
    Insert(&'a [ObjectName], &'a MergeInsertKind),
}

/// One clause with its optional predicate rewritten to scratch-table refs.
pub(crate) struct ClassifiedClause<'a> {
    pub predicate: Option<String>,
    pub op: MergeOp<'a>,
}

/// Statement-ordered clauses grouped by row class.
pub(crate) struct ClassifiedMerge<'a> {
    pub matched: Vec<ClassifiedClause<'a>>,
    pub not_matched: Vec<ClassifiedClause<'a>>,
    pub by_source: Vec<ClassifiedClause<'a>>,
}

impl ClassifiedMerge<'_> {
    pub(crate) fn counts(&self) -> (usize, usize, usize) {
        (
            self.matched.len(),
            self.not_matched.len(),
            self.by_source.len(),
        )
    }
}

/// Rewrite user alias references (`t.`, `s.`) in an expression's SQL
/// rendering to the scratch table names, matching how the ON condition is
/// rewritten in the handler.
fn rewrite_aliases(expr_sql: String, names: &MergeNames<'_>) -> String {
    let out = replace_alias_qualifier(&expr_sql, names.t_alias, names.target_ref);
    replace_alias_qualifier(&out, names.s_alias, names.source_ref)
}

/// Replace `alias.` qualifier occurrences in an expression's SQL rendering
/// with `replacement.`, respecting identifier boundaries and quoting:
///
/// - an occurrence only fires when the preceding character cannot extend an
///   identifier or dotted path (so alias `s` does not fire inside
///   `users.name` or `a.s.x`),
/// - single-quoted string literals and double-quoted identifiers are copied
///   verbatim (with doubled-quote escapes), so `'s.'` in a literal survives.
pub(crate) fn replace_alias_qualifier(sql: &str, alias: &str, replacement: &str) -> String {
    let needle = format!("{alias}.");
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    // True when the previous character could extend an identifier or dotted
    // path, which disqualifies a match starting here.
    let mut guarded = false;
    while i < sql.len() {
        let c = sql[i..].chars().next().expect("index is a char boundary");
        if c == '\'' || c == '"' {
            // Copy the whole quoted region verbatim; a doubled quote escapes.
            let mut j = i + c.len_utf8();
            loop {
                match sql[j..].find(c) {
                    Some(k) => {
                        j += k + c.len_utf8();
                        if sql[j..].starts_with(c) {
                            j += c.len_utf8();
                        } else {
                            break;
                        }
                    }
                    None => {
                        j = sql.len();
                        break;
                    }
                }
            }
            out.push_str(&sql[i..j]);
            i = j;
            // A closing identifier quote can be followed by a member access
            // (`"s".x`); a string literal cannot start an alias either way.
            guarded = true;
            continue;
        }
        if !guarded && sql[i..].starts_with(&needle) {
            out.push_str(replacement);
            out.push('.');
            i += needle.len();
            // The replacement ends in `.`; what follows is a member access.
            guarded = true;
            continue;
        }
        out.push(c);
        guarded = c.is_ascii_alphanumeric() || matches!(c, '_' | '$' | '.');
        i += c.len_utf8();
    }
    out
}

/// Classify the statement's clauses by row class, preserving statement order
/// within each class and rewriting clause predicates to scratch-table refs.
pub(crate) fn classify_merge_clauses<'a>(
    clauses: &'a [MergeClause],
    names: &MergeNames<'_>,
) -> sqe_core::Result<ClassifiedMerge<'a>> {
    let mut out = ClassifiedMerge {
        matched: Vec::new(),
        not_matched: Vec::new(),
        by_source: Vec::new(),
    };
    for clause in clauses {
        // Oracle-style per-action sub-predicates (`UPDATE SET ... WHERE`,
        // `... DELETE WHERE`, `INSERT ... VALUES (...) WHERE`) are parsed by
        // sqlparser but not implemented here. Reject them rather than silently
        // dropping the condition, which would apply the action unconditionally.
        match &clause.action {
            MergeAction::Update(u)
                if u.update_predicate.is_some() || u.delete_predicate.is_some() =>
            {
                return Err(SqeError::NotImplemented(
                    "Oracle-style sub-predicates in a MERGE UPDATE clause \
                     (UPDATE ... WHERE / DELETE WHERE) are not supported; \
                     use WHEN MATCHED AND <cond> instead"
                        .to_string(),
                ));
            }
            MergeAction::Insert(i) if i.insert_predicate.is_some() => {
                return Err(SqeError::NotImplemented(
                    "Oracle-style INSERT ... WHERE in a MERGE clause is not \
                     supported; use WHEN NOT MATCHED AND <cond> instead"
                        .to_string(),
                ));
            }
            _ => {}
        }
        let predicate = clause
            .predicate
            .as_ref()
            .map(|p| rewrite_aliases(format!("{p}"), names));
        let (class, op) = match (&clause.clause_kind, &clause.action) {
            // sqlparser 0.62: MergeAction::Update is a tuple variant holding
            // MergeUpdateExpr; Delete is a struct variant carrying a token.
            (MergeClauseKind::Matched, MergeAction::Update(update_expr)) => {
                (&mut out.matched, MergeOp::Update(&update_expr.assignments))
            }
            (MergeClauseKind::Matched, MergeAction::Delete { .. }) => {
                (&mut out.matched, MergeOp::Delete)
            }
            (
                MergeClauseKind::NotMatched | MergeClauseKind::NotMatchedByTarget,
                MergeAction::Insert(insert_expr),
            ) => (
                &mut out.not_matched,
                MergeOp::Insert(&insert_expr.columns, &insert_expr.kind),
            ),
            (MergeClauseKind::NotMatchedBySource, MergeAction::Update(update_expr)) => (
                &mut out.by_source,
                MergeOp::Update(&update_expr.assignments),
            ),
            (MergeClauseKind::NotMatchedBySource, MergeAction::Delete { .. }) => {
                (&mut out.by_source, MergeOp::Delete)
            }
            (kind, action) => {
                return Err(SqeError::NotImplemented(format!(
                    "Unsupported MERGE clause combination: {kind:?} / {action:?}"
                )));
            }
        };
        class.push(ClassifiedClause { predicate, op });
    }
    Ok(out)
}

/// Is a MERGE whose source returned zero rows a genuine no-op?
///
/// Only when the statement has no `WHEN NOT MATCHED BY SOURCE` clause
/// (architecture review 501). With an empty source every target row is
/// unmatched-by-source, so a BY SOURCE clause must still run: the canonical
/// "make the target equal to the source" MERGE has to empty the target when
/// the upstream extract came back empty, not silently leave it alone.
///
/// The parameter is the *classification result* on purpose: both MERGE
/// handlers can only reach this decision after `classify_merge_clauses` has
/// run, which is the ordering the fix depends on. A future refactor that
/// moves the emptiness shortcut back above classification cannot call this
/// function, so the type system carries the invariant.
pub(crate) fn empty_source_is_no_op(classified: &ClassifiedMerge<'_>) -> bool {
    classified.by_source.is_empty()
}

/// COUNT query for the rows a MERGE with an *empty source* changes.
///
/// With no source rows there is nothing to match and nothing to insert, so
/// the only rows the statement touches are the target rows a BY SOURCE clause
/// claims. The anti-join collapses to a plain scan of the target relation and
/// the surviving filters are the caller's row filter (hidden rows are out of
/// scope, 202) and the union of the clause predicates: first-match-wins makes
/// the union exact, because a row is claimed exactly when at least one clause
/// predicate passes.
///
/// The copy-on-write handler needs this because its normal affected-row count
/// is the number of rows it *wrote*, which is zero after a BY SOURCE DELETE
/// emptied the table. Returns `None` when there are no BY SOURCE clauses (the
/// statement is a no-op and the handler returns before committing).
pub(crate) fn empty_source_claim_count_sql(
    classified: &ClassifiedMerge<'_>,
    names: &MergeNames<'_>,
    qualified_target_ref: &str,
) -> Option<String> {
    if classified.by_source.is_empty() {
        return None;
    }
    // An unpredicated clause claims every row not already claimed, so the
    // union degenerates to TRUE as soon as one appears.
    let claimed = if classified.by_source.iter().any(|c| c.predicate.is_none()) {
        "TRUE".to_string()
    } else {
        classified
            .by_source
            .iter()
            .filter_map(|c| c.predicate.as_ref())
            .map(|p| format!("({p})"))
            .collect::<Vec<_>>()
            .join(" OR ")
    };
    let admit = admit_guard(names)
        .map(|g| format!(" AND {g}"))
        .unwrap_or_default();
    let relation = target_relation(names, qualified_target_ref);
    Some(format!(
        "SELECT COUNT(*) FROM {relation} AS {} WHERE ({claimed}){admit}",
        names.target_ref
    ))
}

fn case_arm(class_cond: &str, predicate: Option<&str>, value: &str) -> String {
    match predicate {
        Some(p) => format!("WHEN {class_cond} AND ({p}) THEN {value}"),
        None => format!("WHEN {class_cond} THEN {value}"),
    }
}

/// Name of the synthetic per-side presence-flag column used to detect a row's
/// match class. Each side of the FULL OUTER JOIN is wrapped in a derived table
/// that adds `TRUE AS <flag>`; the flag is NULL exactly when that side is
/// absent for the joined row. Derived from the unique per-invocation scratch
/// ref so it cannot collide with a user column.
fn present_flag(scratch_ref: &str) -> String {
    format!("__sqe_present_{scratch_ref}")
}

/// Build the merge SELECT: an inner FULL OUTER JOIN projection with one CASE
/// per target column plus the keep column, wrapped in an outer projection
/// that filters on the keep column and drops it.
///
/// Match classes are detected through synthetic presence-flag columns
/// ([`present_flag`]) injected on each side, not through any user column. A
/// row present on a side has a non-NULL flag; an absent side (the NULL half of
/// the outer join) has a NULL flag. So a genuinely NULL data column can no
/// longer misclassify a present row.
pub(crate) fn build_merge_select(
    classified: &ClassifiedMerge<'_>,
    names: &MergeNames<'_>,
    qualified_target_ref: &str,
    qualified_source_ref: &str,
    on_rewritten: &str,
) -> String {
    merge_select(
        classified,
        names,
        qualified_target_ref,
        qualified_source_ref,
        on_rewritten,
        SelectMode::Rewrite,
    )
}

/// What [`merge_select`] projects over the classified FULL OUTER JOIN.
enum SelectMode<'a> {
    /// The surviving target rows ([`build_merge_select`]).
    Rewrite,
    /// COUNT of the claimed, kept rows outside this row filter
    /// ([`build_merge_with_check_sql`]).
    WithCheck(&'a str),
    /// COUNT of the rows some clause claims ([`build_merge_claimed_count_sql`]).
    ClaimedCount,
}

/// [`build_merge_select`], or one of the COUNT queries over the same row
/// classes (see [`SelectMode`]).
fn merge_select(
    classified: &ClassifiedMerge<'_>,
    names: &MergeNames<'_>,
    qualified_target_ref: &str,
    qualified_source_ref: &str,
    on_rewritten: &str,
    mode: SelectMode<'_>,
) -> String {
    let t_flag = present_flag(names.target_ref);
    let s_flag = present_flag(names.source_ref);
    let target_sentinel = format!("{}.\"{t_flag}\"", names.target_ref);
    let source_sentinel = format!("{}.\"{s_flag}\"", names.source_ref);
    let matched_cond = format!("{target_sentinel} IS NOT NULL AND {source_sentinel} IS NOT NULL");
    let source_only_cond = format!("{target_sentinel} IS NULL");
    // A hidden target row (row filter says no) is target-only by construction
    // (the guarded ON never matched it) and must not be claimed by a
    // NOT MATCHED BY SOURCE clause either: it falls to the passthrough ELSE.
    let target_only_cond = match admit_guard(names) {
        Some(guard) => format!("{source_sentinel} IS NULL AND {guard}"),
        None => format!("{source_sentinel} IS NULL"),
    };
    let on_rewritten = guarded_on(names, on_rewritten);
    let target_relation = target_relation(names, qualified_target_ref);

    let mut inner_cols: Vec<String> = Vec::with_capacity(names.target_columns.len() + 1);
    for col in names.target_columns {
        let passthrough = raw_target_col(names, col);
        // V3 row lineage (present only when the caller added the reserved
        // names): a row keeps `_row_id` unless inserted, and gets a NULL
        // `_last_updated_sequence_number` when inserted or updated so it
        // inherits the new file's sequence number.
        let is_row_id = col == iceberg::metadata_columns::RESERVED_COL_NAME_ROW_ID;
        let is_last_updated =
            col == iceberg::metadata_columns::RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER;
        if is_row_id || is_last_updated {
            const NULL: &str = "CAST(NULL AS BIGINT)";
            let updated = if is_last_updated { NULL } else { passthrough.as_str() };
            let mut arms: Vec<String> = Vec::new();
            for clause in &classified.matched {
                let v = if matches!(clause.op, MergeOp::Update(_)) { updated } else { passthrough.as_str() };
                arms.push(case_arm(&matched_cond, clause.predicate.as_deref(), v));
            }
            for clause in &classified.not_matched {
                arms.push(case_arm(&source_only_cond, clause.predicate.as_deref(), NULL));
            }
            for clause in &classified.by_source {
                let v = if matches!(clause.op, MergeOp::Update(_)) { updated } else { passthrough.as_str() };
                arms.push(case_arm(&target_only_cond, clause.predicate.as_deref(), v));
            }
            inner_cols.push(format!(
                "CASE {} ELSE {passthrough} END AS \"{col}\"",
                arms.join(" ")
            ));
            continue;
        }
        let mut arms: Vec<String> = Vec::new();
        for clause in &classified.matched {
            // DELETE rows are dropped by the keep filter; project the target
            // column so every arm of the CASE keeps the column's type.
            let value = match &clause.op {
                MergeOp::Update(assignments) => resolve_update_expr(col, assignments, names),
                _ => passthrough.clone(),
            };
            arms.push(case_arm(&matched_cond, clause.predicate.as_deref(), &value));
        }
        for clause in &classified.not_matched {
            let value = match &clause.op {
                MergeOp::Insert(insert_cols, insert_kind) => resolve_insert_expr(
                    col,
                    insert_cols,
                    insert_kind,
                    names.source_ref,
                    names.source_columns,
                    names.s_alias,
                    names.t_alias,
                    names.target_ref,
                    omitted_value(names, col),
                ),
                _ => passthrough.clone(),
            };
            arms.push(case_arm(
                &source_only_cond,
                clause.predicate.as_deref(),
                &value,
            ));
        }
        for clause in &classified.by_source {
            let value = match &clause.op {
                // BY SOURCE UPDATE may only reference target columns per the
                // SQL standard; a source reference resolves to NULL here
                // (the source side is absent for this row class).
                MergeOp::Update(assignments) => resolve_update_expr(col, assignments, names),
                _ => passthrough.clone(),
            };
            arms.push(case_arm(
                &target_only_cond,
                clause.predicate.as_deref(),
                &value,
            ));
        }
        inner_cols.push(format!(
            "CASE {} ELSE {passthrough} END AS \"{col}\"",
            arms.join(" ")
        ));
    }

    // Keep column: same arm order as the data columns so the same clause
    // decides both. Class defaults: unclaimed matched and target-only rows
    // pass through (TRUE via the class arm / ELSE), unclaimed source-only
    // rows are not inserted (FALSE).
    let mut keep_arms: Vec<String> = Vec::new();
    for clause in &classified.matched {
        let keep = if matches!(clause.op, MergeOp::Delete) {
            "FALSE"
        } else {
            "TRUE"
        };
        keep_arms.push(case_arm(&matched_cond, clause.predicate.as_deref(), keep));
    }
    for clause in &classified.not_matched {
        keep_arms.push(case_arm(
            &source_only_cond,
            clause.predicate.as_deref(),
            "TRUE",
        ));
    }
    keep_arms.push(format!("WHEN {source_only_cond} THEN FALSE"));
    for clause in &classified.by_source {
        let keep = if matches!(clause.op, MergeOp::Delete) {
            "FALSE"
        } else {
            "TRUE"
        };
        keep_arms.push(case_arm(
            &target_only_cond,
            clause.predicate.as_deref(),
            keep,
        ));
    }
    inner_cols.push(format!(
        "CASE {} ELSE TRUE END AS {MERGE_KEEP_COLUMN}",
        keep_arms.join(" ")
    ));

    // The COUNT variants flag the rows some clause claimed (same first-match
    // arms). WITH CHECK counts the kept ones whose new values the caller's
    // row filter rejects; unclaimed rows pass through unchanged and are not
    // checked. The claimed count includes DELETE rows (keep is FALSE).
    let claimed_arms: Vec<String> = classified
        .matched
        .iter()
        .map(|c| case_arm(&matched_cond, c.predicate.as_deref(), "TRUE"))
        .chain(
            classified
                .not_matched
                .iter()
                .map(|c| case_arm(&source_only_cond, c.predicate.as_deref(), "TRUE")),
        )
        .chain(
            classified
                .by_source
                .iter()
                .map(|c| case_arm(&target_only_cond, c.predicate.as_deref(), "TRUE")),
        )
        .collect();
    let claimed = if claimed_arms.is_empty() {
        "FALSE".to_string()
    } else {
        format!("CASE {} ELSE FALSE END", claimed_arms.join(" "))
    };
    let outer_projection = match mode {
        SelectMode::WithCheck(rf) => {
            inner_cols.push(format!("{claimed} AS {MERGE_CLAIMED_COLUMN}"));
            (
                "COUNT(*)".to_string(),
                format!(
                    "{MERGE_KEEP_COLUMN} AND {MERGE_CLAIMED_COLUMN} \
                     AND NOT COALESCE(CAST(({rf}) AS BOOLEAN), FALSE)"
                ),
            )
        }
        SelectMode::ClaimedCount => {
            inner_cols.push(format!("{claimed} AS {MERGE_CLAIMED_COLUMN}"));
            ("COUNT(*)".to_string(), MERGE_CLAIMED_COLUMN.to_string())
        }
        SelectMode::Rewrite => (
            names
                .target_columns
                .iter()
                .map(|c| format!("\"{c}\""))
                .collect::<Vec<_>>()
                .join(", "),
            MERGE_KEEP_COLUMN.to_string(),
        ),
    };
    let (outer_projection, outer_filter) = outer_projection;

    // Wrap each side in a derived table that adds the presence flag, so match
    // class depends on the flag rather than a nullable user column. The derived
    // tables keep the scratch-ref aliases, so the ON condition, passthrough,
    // and assignment references (`{ref}."col"`) all still resolve; the flag
    // columns stay inside the join scope and never reach the outer projection.
    format!(
        "SELECT {outer_projection} FROM (SELECT {} FROM \
         (SELECT *, TRUE AS \"{t_flag}\" FROM {target_relation}) AS {} \
         FULL OUTER JOIN \
         (SELECT *, TRUE AS \"{s_flag}\" FROM {qualified_source_ref}) AS {} \
         ON {on_rewritten}) \
         WHERE {outer_filter}",
        inner_cols.join(", "),
        names.target_ref,
        names.source_ref,
    )
}

/// WITH CHECK for MERGE (as for INSERT, #729): a COUNT of the rows the
/// statement inserts or updates whose new values fall outside the caller's
/// row filter on the target, or `None` without a row filter. Non-zero means
/// the statement must be refused.
pub(crate) fn build_merge_with_check_sql(
    classified: &ClassifiedMerge<'_>,
    names: &MergeNames<'_>,
    qualified_target_ref: &str,
    qualified_source_ref: &str,
    on_rewritten: &str,
) -> Option<String> {
    let rf = names.target_policy.row_filter_sql.as_deref()?;
    Some(merge_select(
        classified,
        names,
        qualified_target_ref,
        qualified_source_ref,
        on_rewritten,
        SelectMode::WithCheck(rf),
    ))
}

/// The copy-on-write MERGE affected-row count: a COUNT of the rows some
/// clause claims (updated, deleted or inserted). The rewrite itself only
/// sees the rows that survive, so its row count is the size of the new table,
/// not what the statement changed.
pub(crate) fn build_merge_claimed_count_sql(
    classified: &ClassifiedMerge<'_>,
    names: &MergeNames<'_>,
    qualified_target_ref: &str,
    qualified_source_ref: &str,
    on_rewritten: &str,
) -> String {
    merge_select(
        classified,
        names,
        qualified_target_ref,
        qualified_source_ref,
        on_rewritten,
        SelectMode::ClaimedCount,
    )
}

/// Build the two COUNT queries used to detect a MERGE cardinality violation
/// (a target row matched by more than one source row). Returns
/// `(pair_count_sql, matched_count_sql)`:
///
/// - `pair_count` counts join output rows (one per matching target/source
///   pair),
/// - `matched_count` counts distinct target rows that have at least one match.
///
/// `pair_count > matched_count` iff some target row matched multiple source
/// rows. Using two counts avoids materialising a synthetic row id, so the
/// check stays a cheap streaming aggregate. Only meaningful when the statement
/// has at least one matched clause.
pub(crate) fn build_cardinality_check_sql(
    names: &MergeNames<'_>,
    qualified_target_ref: &str,
    qualified_source_ref: &str,
    on_rewritten: &str,
) -> (String, String) {
    let t = names.target_ref;
    let s = names.source_ref;
    let target = target_relation(names, qualified_target_ref);
    let on_rewritten = guarded_on(names, on_rewritten);
    let pair_count = format!(
        "SELECT COUNT(*) FROM {target} AS {t} \
         JOIN {qualified_source_ref} AS {s} ON {on_rewritten}"
    );
    let matched_count = format!(
        "SELECT COUNT(*) FROM {target} AS {t} \
         WHERE EXISTS (SELECT 1 FROM {qualified_source_ref} AS {s} WHERE {on_rewritten})"
    );
    (pair_count, matched_count)
}

/// First-match-wins guard for the clause at `idx` within one row class
/// (matched / not-matched / by-source), for the merge-on-read path which runs
/// one guarded query per clause instead of a single CASE. The clause fires
/// only when its own predicate holds and no earlier clause in the class
/// already claimed the row:
///
/// `COALESCE((own), FALSE) AND NOT COALESCE((prior_0), FALSE) AND ...`
///
/// NULL predicates count as false (SQL CASE semantics). An unconditional
/// clause (no predicate) contributes no own-term (it matches every row in the
/// class); once an unconditional clause appears, every later clause in the
/// class is unreachable and its guard collapses to `FALSE`, mirroring the CoW
/// CASE where a later arm after an unconditional one never runs.
pub(crate) fn first_match_guard(class: &[ClassifiedClause<'_>], idx: usize) -> String {
    let mut terms: Vec<String> = Vec::new();
    if let Some(p) = &class[idx].predicate {
        terms.push(format!("COALESCE(({p}), FALSE)"));
    }
    for prior in &class[..idx] {
        match &prior.predicate {
            Some(p) => terms.push(format!("NOT COALESCE(({p}), FALSE)")),
            // A prior unconditional clause already claimed the row.
            None => terms.push("FALSE".to_string()),
        }
    }
    if terms.is_empty() {
        "TRUE".to_string()
    } else {
        terms.join(" AND ")
    }
}

/// Resolve the UPDATE expression for one target column from the SET
/// assignments, rewriting alias references to the scratch table names. A
/// column without an assignment passes through its stored (raw) value.
pub(crate) fn resolve_update_expr(
    col: &str,
    assignments: &[Assignment],
    names: &MergeNames<'_>,
) -> String {
    for a in assignments {
        let col_name = match &a.target {
            sqlparser::ast::AssignmentTarget::ColumnName(name) => {
                // Could be "t.col" or just "col"
                let parts: Vec<String> = name
                    .0
                    .iter()
                    .filter_map(|p| p.as_ident())
                    .map(|i| i.value.clone())
                    .collect();
                parts.last().cloned().unwrap_or_default()
            }
            sqlparser::ast::AssignmentTarget::Tuple(names) => names
                .first()
                .map(|n| {
                    let parts: Vec<String> =
                        n.0.iter()
                            .filter_map(|p| p.as_ident())
                            .map(|i| i.value.clone())
                            .collect();
                    parts.last().cloned().unwrap_or_default()
                })
                .unwrap_or_default(),
        };
        if col_name == col {
            // Rewrite alias references to MemTable names
            return rewrite_aliases(format!("{}", a.value), names);
        }
    }
    // Column not in SET assignments — pass through from target
    raw_target_col(names, col)
}

/// Resolve the INSERT expression for one target column in the MERGE context.
///
/// Maps the INSERT column list + VALUES to find the expression for the
/// given target column. Rewrites alias references (e.g., `s.col`) to
/// use the MemTable name. A column the clause does not supply gets
/// `omitted`: its `write_default` or NULL ([`omitted_value`]).
#[allow(clippy::too_many_arguments)]
pub(crate) fn resolve_insert_expr(
    col: &str,
    // sqlparser 0.62: MERGE INSERT column list is Vec<ObjectName> (was Vec<Ident>).
    insert_columns: &[ObjectName],
    insert_kind: &MergeInsertKind,
    source_table_ref: &str,
    source_columns: &[String],
    s_alias: &str,
    t_alias: &str,
    target_table_ref: &str,
    omitted: &str,
) -> String {
    let rewrite = |expr: String| -> String {
        let out = replace_alias_qualifier(&expr, s_alias, source_table_ref);
        replace_alias_qualifier(&out, t_alias, target_table_ref)
    };

    match insert_kind {
        MergeInsertKind::Values(values) => {
            if insert_columns.is_empty() {
                // No explicit column list — positional mapping by source column name.
                if let Some(row) = values.rows.first() {
                    if let Some(idx) = source_columns.iter().position(|sc| sc == col) {
                        if idx < row.len() {
                            return rewrite(format!("{}", row[idx]));
                        }
                    }
                    return omitted.to_string();
                }
                omitted.to_string()
            } else {
                // Explicit column list — find the column position. A MERGE
                // insert column is a bare name; compare against its last
                // identifier part (ObjectName in 0.62).
                if let Some(pos) = insert_columns.iter().position(|c| {
                    c.0.last()
                        .and_then(|p| p.as_ident())
                        .map(|id| id.value == col)
                        .unwrap_or(false)
                }) {
                    if let Some(row) = values.rows.first() {
                        if pos < row.len() {
                            return rewrite(format!("{}", row[pos]));
                        }
                    }
                }
                omitted.to_string()
            }
        }
        MergeInsertKind::Row => {
            // INSERT ROW: use the source column with the same name
            if source_columns.contains(&col.to_string()) {
                format!("{source_table_ref}.\"{col}\"")
            } else {
                omitted.to_string()
            }
        }
    }
}

/// What a NOT MATCHED INSERT writes into target column `col` when the clause
/// does not supply it: the column's `write_default`, else NULL (#828).
pub(crate) fn omitted_value<'a>(names: &MergeNames<'a>, col: &str) -> &'a str {
    names.column_defaults.get(col).map_or("NULL", String::as_str)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    fn merge_clauses(sql: &str) -> Vec<MergeClause> {
        let stmts = Parser::parse_sql(&GenericDialect {}, sql).expect("parse");
        match stmts.into_iter().next().expect("one statement") {
            sqlparser::ast::Statement::Merge(m) => m.clauses,
            other => panic!("expected MERGE, got {other}"),
        }
    }

    fn names<'a>(target_cols: &'a [String], source_cols: &'a [String]) -> MergeNames<'a> {
        static NO_POLICY: std::sync::OnceLock<WritePolicyPredicates> = std::sync::OnceLock::new();
        names_with_policy(
            target_cols,
            source_cols,
            NO_POLICY.get_or_init(WritePolicyPredicates::default),
        )
    }

    static NO_DEFAULTS: std::sync::OnceLock<std::collections::HashMap<String, String>> =
        std::sync::OnceLock::new();

    fn names_with_policy<'a>(
        target_cols: &'a [String],
        source_cols: &'a [String],
        policy: &'a WritePolicyPredicates,
    ) -> MergeNames<'a> {
        MergeNames {
            target_ref: "__merge_target_x",
            source_ref: "__merge_source_x",
            t_alias: "t",
            s_alias: "s",
            target_columns: target_cols,
            source_columns: source_cols,
            target_policy: policy,
            column_defaults: NO_DEFAULTS.get_or_init(Default::default),
        }
    }

    fn eu_only_masked_ssn() -> WritePolicyPredicates {
        WritePolicyPredicates {
            row_filter_sql: Some("region = 'EU'".to_string()),
            column_mask_sqls: std::collections::HashMap::from([(
                "ssn".to_string(),
                "'***'".to_string(),
            )]),
            ..Default::default()
        }
    }

    /// 202: the caller's target policy is folded into the merge SQL: hidden
    /// rows never match and are never claimed by BY SOURCE; masked columns
    /// carry the mask under their own name (ON, predicates, SET) and the raw
    /// value only under the engine's passthrough alias.
    #[test]
    fn target_policy_guards_on_by_source_and_masks_user_references() {
        let sql = "MERGE INTO tgt AS t USING src AS s ON t.id = s.id \
                   WHEN MATCHED AND t.ssn = s.ssn THEN UPDATE SET ssn = t.ssn \
                   WHEN NOT MATCHED BY SOURCE THEN DELETE";
        let clauses = merge_clauses(sql);
        let tc = cols(&["id", "region", "ssn"]);
        let sc = cols(&["id", "ssn"]);
        let policy = eu_only_masked_ssn();
        let n = names_with_policy(&tc, &sc, &policy);
        let classified = classify_merge_clauses(&clauses, &n).expect("classify");
        let out = build_merge_select(
            &classified,
            &n,
            "datafusion.public.__merge_target_x",
            "datafusion.public.__merge_source_x",
            "__merge_target_x.id = __merge_source_x.id",
        );
        let admit = format!("__merge_target_x.\"{MERGE_ADMIT_COLUMN}\"");
        assert!(
            out.contains(&format!(
                "ON (__merge_target_x.id = __merge_source_x.id) AND {admit}"
            )),
            "ON must carry the admit guard: {out}"
        );
        let s_flag = present_flag("__merge_source_x");
        assert!(
            out.contains(&format!(
                "WHEN __merge_source_x.\"{s_flag}\" IS NULL AND {admit} THEN FALSE"
            )),
            "BY SOURCE DELETE must be guarded: {out}"
        );
        // Masks never reach the merge SQL itself: it reads the masked view,
        // whose SELECT (planned in the mask context) computes admit, masked
        // and raw columns.
        let view = target_policy_view(&n).expect("masked target has a view");
        assert!(
            out.contains(&format!("FROM {view}")) && !out.contains("'***'"),
            "merge SQL must read the masked view, not inline masks: {out}"
        );
        let relation = target_policy_select(&n, "datafusion.public.__merge_target_x");
        assert!(
            relation.contains("((region = 'EU') IS TRUE) AS \"__sqe_merge_admit\"")
                && relation.contains("CASE WHEN (region = 'EU') THEN ('***') END AS \"ssn\"")
                && relation.contains("CASE WHEN (region = 'EU') THEN \"id\" END AS \"id\"")
                && relation.contains("\"ssn\" AS \"__sqe_raw___merge_target_x_ssn\"")
                && relation.contains("\"id\" AS \"__sqe_raw___merge_target_x_id\""),
            "target relation must compute admit, guarded and raw columns: {relation}"
        );
        assert!(
            out.contains("ELSE __merge_target_x.\"__sqe_raw___merge_target_x_ssn\" END AS \"ssn\""),
            "the engine passthrough keeps the raw value: {out}"
        );
        // Without a policy the SQL is unchanged in shape.
        let plain = build(sql, &["id", "region", "ssn"], &["id", "ssn"]);
        assert!(!plain.contains(MERGE_ADMIT_COLUMN) && !plain.contains("__sqe_raw_"));
    }

    /// 202, executed: a caller filtered to EU deletes an EU row through
    /// MERGE, cannot delete or update a hidden US row, and the hidden row is
    /// rewritten byte-for-byte (CoW rewrites whole files).
    #[tokio::test]
    async fn target_row_filter_protects_hidden_rows_end_to_end() {
        use arrow_array::{Int32Array, StringArray};
        use arrow_schema::{DataType, Field, Schema};
        use datafusion::prelude::SessionContext;
        use std::sync::Arc;

        let ctx = SessionContext::new();
        let tschema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("region", DataType::Utf8, false),
            Field::new("amount", DataType::Int32, false),
        ]));
        let target = arrow_array::RecordBatch::try_new(
            tschema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["EU", "US", "US"])),
                Arc::new(Int32Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();
        let sschema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let source = arrow_array::RecordBatch::try_new(
            sschema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2]))],
        )
        .unwrap();
        ctx.register_table(
            "datafusion.public.__merge_target_x",
            Arc::new(
                datafusion::datasource::MemTable::try_new(tschema, vec![vec![target]]).unwrap(),
            ),
        )
        .unwrap();
        ctx.register_table(
            "datafusion.public.__merge_source_x",
            Arc::new(
                datafusion::datasource::MemTable::try_new(sschema, vec![vec![source]]).unwrap(),
            ),
        )
        .unwrap();

        let try_run = |sql: &str, on: &str, policy: WritePolicyPredicates| {
            let ctx = ctx.clone();
            let sql = sql.to_string();
            let on = on.to_string();
            async move {
                let clauses = merge_clauses(&sql);
                let tc = cols(&["id", "region", "amount"]);
                let sc = cols(&["id"]);
                let n = names_with_policy(&tc, &sc, &policy);
                let classified = classify_merge_clauses(&clauses, &n).unwrap();
                let select = build_merge_select(
                    &classified,
                    &n,
                    "datafusion.public.__merge_target_x",
                    "datafusion.public.__merge_source_x",
                    &on,
                );
                let batches = match ctx.sql(&select).await {
                    Ok(df) => df.collect().await,
                    Err(e) => Err(e),
                }
                .map_err(|e| e.to_string())?;
                let mut rows: Vec<(i32, String, i32)> = Vec::new();
                for b in &batches {
                    // CASE arms coerce types (literal 0 -> Int64, Utf8 -> Utf8View);
                    // normalise before downcasting so the assert is about values.
                    let ids = arrow::compute::cast(b.column(0), &DataType::Int32).unwrap();
                    let ids = ids.as_any().downcast_ref::<Int32Array>().unwrap();
                    let regions = arrow::compute::cast(b.column(1), &DataType::Utf8).unwrap();
                    let regions = regions.as_any().downcast_ref::<StringArray>().unwrap();
                    let amounts = arrow::compute::cast(b.column(2), &DataType::Int32).unwrap();
                    let amounts = amounts.as_any().downcast_ref::<Int32Array>().unwrap();
                    for i in 0..b.num_rows() {
                        rows.push((ids.value(i), regions.value(i).to_string(), amounts.value(i)));
                    }
                }
                rows.sort();
                Ok::<_, String>(rows)
            }
        };
        let on = "__merge_target_x.id = __merge_source_x.id";
        let run = |sql: &str, policy: WritePolicyPredicates| {
            let fut = try_run(sql, on, policy);
            async move { fut.await.unwrap() }
        };
        let eu_only = || WritePolicyPredicates {
            row_filter_sql: Some("region = 'EU'".to_string()),
            ..Default::default()
        };

        // A fallible ON / clause predicate / SET never runs on a hidden row:
        // through the row-filter guard a hidden row is all NULL. The same
        // MERGE without the policy divides by zero on the US rows (amount 20
        // in ON and MATCHED, amount 30 in BY SOURCE).
        let fallible = "MERGE INTO tgt AS t USING src AS s ON t.id = s.id \
                        WHEN MATCHED AND 1 / (t.amount - 20) <> 7 THEN UPDATE SET amount = 1 / (t.amount - 20) \
                        WHEN NOT MATCHED BY SOURCE AND 1 / (t.amount - 30) = 1 THEN DELETE";
        let fallible_on = format!("({on}) AND 1 / (__merge_target_x.amount - 20) <> 7");
        let err = try_run(fallible, &fallible_on, WritePolicyPredicates::default())
            .await
            .expect_err("unguarded, the predicate fails on the US rows");
        assert!(err.contains("Divide by zero"), "{err}");
        assert_eq!(
            try_run(fallible, &fallible_on, eu_only()).await.unwrap(),
            vec![
                (1, "EU".to_string(), 0),
                (2, "US".to_string(), 20),
                (3, "US".to_string(), 30)
            ],
            "the visible row is updated; hidden rows pass through unchanged"
        );

        let delete = "MERGE INTO tgt AS t USING src AS s ON t.id = s.id WHEN MATCHED THEN DELETE";
        assert_eq!(
            run(delete, WritePolicyPredicates::default()).await,
            vec![(3, "US".to_string(), 30)],
            "unrestricted: both matched rows deleted"
        );
        assert_eq!(
            run(delete, eu_only()).await,
            vec![(2, "US".to_string(), 20), (3, "US".to_string(), 30)],
            "EU-only caller deletes the EU row; the hidden US row survives untouched"
        );

        let update = "MERGE INTO tgt AS t USING src AS s ON t.id = s.id \
                      WHEN MATCHED THEN UPDATE SET amount = 0 \
                      WHEN NOT MATCHED BY SOURCE THEN UPDATE SET amount = -1";
        assert_eq!(
            run(update, eu_only()).await,
            vec![
                (1, "EU".to_string(), 0),
                (2, "US".to_string(), 20),
                (3, "US".to_string(), 30)
            ],
            "hidden rows are neither matched nor claimed by NOT MATCHED BY SOURCE"
        );

        // WITH CHECK: rows a clause updates or inserts must stay inside the
        // caller's row filter; unclaimed hidden rows are not checked.
        let violations = |sql: &str| {
            let ctx = ctx.clone();
            let sql = sql.to_string();
            async move {
                let clauses = merge_clauses(&sql);
                let tc = cols(&["id", "region", "amount"]);
                let sc = cols(&["id"]);
                let policy = eu_only();
                let n = names_with_policy(&tc, &sc, &policy);
                let classified = classify_merge_clauses(&clauses, &n).unwrap();
                let check = build_merge_with_check_sql(
                    &classified,
                    &n,
                    "datafusion.public.__merge_target_x",
                    "datafusion.public.__merge_source_x",
                    "__merge_target_x.id = __merge_source_x.id",
                )
                .expect("row filter present");
                let b = ctx.sql(&check).await.unwrap().collect().await.unwrap();
                let c = arrow::compute::cast(b[0].column(0), &DataType::Int64).unwrap();
                c.as_any().downcast_ref::<arrow_array::Int64Array>().unwrap().value(0)
            }
        };
        assert_eq!(violations(update).await, 0, "in-filter update passes");
        assert_eq!(
            violations(
                "MERGE INTO tgt AS t USING src AS s ON t.id = s.id \
                 WHEN MATCHED THEN UPDATE SET region = 'US'"
            )
            .await,
            1,
            "moving the visible EU row out of the filter is a violation"
        );
        assert_eq!(
            violations(
                "MERGE INTO tgt AS t USING src AS s ON t.id = s.id \
                 WHEN NOT MATCHED THEN INSERT (id, region, amount) VALUES (s.id, 'US', 0)"
            )
            .await,
            1,
            "inserting a row the caller could not see is a violation"
        );
    }

    /// A masked target column is read masked however the user names it
    /// (bare or qualified), while the stored value of an unassigned masked
    /// column is written back raw.
    #[tokio::test]
    async fn bare_masked_column_reads_masked_and_passthrough_stays_raw() {
        use arrow_array::{Int32Array, StringArray};
        use arrow_schema::{DataType, Field, Schema};
        use datafusion::prelude::SessionContext;
        use std::sync::Arc;

        let ctx = SessionContext::new();
        let tschema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("ssn", DataType::Utf8, false),
            Field::new("note", DataType::Utf8, false),
        ]));
        let target = arrow_array::RecordBatch::try_new(
            tschema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["111", "222"])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();
        let sschema = Arc::new(Schema::new(vec![Field::new("sid", DataType::Int32, false)]));
        let source = arrow_array::RecordBatch::try_new(
            sschema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2]))],
        )
        .unwrap();
        for (name, schema, batch) in [
            ("datafusion.public.__merge_target_x", tschema, target),
            ("datafusion.public.__merge_source_x", sschema, source),
        ] {
            let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
            ctx.register_table(name, Arc::new(mem)).unwrap();
        }

        let sql = "MERGE INTO tgt AS t USING src AS s ON t.id = s.sid \
                   WHEN MATCHED AND ssn = '111' THEN DELETE \
                   WHEN MATCHED THEN UPDATE SET note = ssn";
        let clauses = merge_clauses(sql);
        let tc = cols(&["id", "ssn", "note"]);
        let sc = cols(&["sid"]);
        let policy = WritePolicyPredicates {
            row_filter_sql: None,
            column_mask_sqls: std::collections::HashMap::from([(
                "ssn".to_string(),
                "'***'".to_string(),
            )]),
            ..Default::default()
        };
        let n = names_with_policy(&tc, &sc, &policy);
        register_target_policy_view(&ctx, &n, "datafusion.public.__merge_target_x")
            .await
            .unwrap();
        let classified = classify_merge_clauses(&clauses, &n).unwrap();
        let select = build_merge_select(
            &classified,
            &n,
            "datafusion.public.__merge_target_x",
            "datafusion.public.__merge_source_x",
            "__merge_target_x.id = __merge_source_x.sid",
        );
        let batches = ctx.sql(&select).await.unwrap().collect().await.unwrap();
        let mut rows: Vec<(i32, String, String)> = Vec::new();
        for b in &batches {
            let ids = arrow::compute::cast(b.column(0), &DataType::Int32).unwrap();
            let ids = ids.as_any().downcast_ref::<Int32Array>().unwrap();
            let ssn = arrow::compute::cast(b.column(1), &DataType::Utf8).unwrap();
            let ssn = ssn.as_any().downcast_ref::<StringArray>().unwrap();
            let note = arrow::compute::cast(b.column(2), &DataType::Utf8).unwrap();
            let note = note.as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..b.num_rows() {
                rows.push((ids.value(i), ssn.value(i).to_string(), note.value(i).to_string()));
            }
        }
        rows.sort();
        assert_eq!(
            rows,
            vec![
                (1, "111".to_string(), "***".to_string()),
                (2, "222".to_string(), "***".to_string()),
            ],
            "predicate and SET read the mask; stored ssn is kept raw"
        );
    }

    /// A HASH mask calls the internal keyed function, which the session that
    /// plans the merge SQL (and the user's ON / SET / USING text) never
    /// registers. The masked target view is planned in the separate mask
    /// context, so MERGE plans and reads the keyed hash, while user SQL
    /// naming the function still fails to plan.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn keyed_hash_mask_merges_without_exposing_mask_udf() {
        use arrow_array::{Int32Array, StringArray};
        use arrow_schema::{DataType, Field, Schema};
        use datafusion::prelude::SessionContext;
        use sqe_policy::plan_rewriter::PolicyPlanRewriter;
        use sqe_policy::policy_store::InMemoryPolicyStore;
        use sqe_policy::sha256_udf::{sha256_udf, MASK_HASH_UDF_NAME};
        use sqe_policy::{MaskType, ResolvedPolicy};
        use std::sync::Arc;

        let ctx = SessionContext::new();
        let tschema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("ssn", DataType::Utf8, false),
            Field::new("note", DataType::Utf8, false),
        ]));
        let target = arrow_array::RecordBatch::try_new(
            tschema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["111", "222"])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();
        let sschema = Arc::new(Schema::new(vec![Field::new("sid", DataType::Int32, false)]));
        let source = arrow_array::RecordBatch::try_new(
            sschema.clone(),
            vec![Arc::new(Int32Array::from(vec![1]))],
        )
        .unwrap();
        for (name, schema, batch) in [
            ("datafusion.public.__merge_target_x", tschema.clone(), target),
            ("datafusion.public.__merge_source_x", sschema, source),
        ] {
            let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
            ctx.register_table(name, Arc::new(mem)).unwrap();
        }

        // The real rewriter, so the mask text and its function are the ones
        // production hands the write path.
        let key = Arc::new(b"mask-key".to_vec());
        let store = InMemoryPolicyStore::new();
        let mut pol = ResolvedPolicy::default();
        pol.column_masks.insert("ssn".to_string(), MaskType::Hash);
        store.add_table_policy("default", "tgt", pol).await;
        let enf = PolicyPlanRewriter::new(Arc::new(store)).with_mask_key(Some(key.clone()));
        let user = sqe_core::SessionUser {
            username: "alice".to_string(),
            roles: vec![],
            subject: None,
            email: None,
            groups: vec![],
        };
        let (policy, _) = sqe_policy::write_predicates::extract_write_predicates(
            &enf, &user, None, "default", "tgt", tschema,
        )
        .await
        .unwrap();

        let keyed = {
            let c = SessionContext::new();
            c.register_udf(sha256_udf(Some(key)));
            let b = c
                .sql(&format!("SELECT {MASK_HASH_UDF_NAME}('111')"))
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            let a = arrow::compute::cast(b[0].column(0), &DataType::Utf8).unwrap();
            a.as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0)
                .to_string()
        };

        let tc = cols(&["id", "ssn", "note"]);
        let sc = cols(&["sid"]);
        let n = names_with_policy(&tc, &sc, &policy);
        let plan_merge = |sql: &str| {
            let clauses = merge_clauses(sql);
            let classified = classify_merge_clauses(&clauses, &n).unwrap();
            build_merge_select(
                &classified,
                &n,
                "datafusion.public.__merge_target_x",
                "datafusion.public.__merge_source_x",
                "__merge_target_x.id = __merge_source_x.sid",
            )
        };
        let update = plan_merge(
            "MERGE INTO tgt AS t USING src AS s ON t.id = s.sid \
             WHEN MATCHED THEN UPDATE SET note = ssn",
        );
        assert!(
            ctx.sql(&update).await.is_err(),
            "without the mask-context view the merge must fail to plan, never fall back"
        );

        register_target_policy_view(&ctx, &n, "datafusion.public.__merge_target_x")
            .await
            .unwrap();
        let batches = ctx.sql(&update).await.unwrap().collect().await.unwrap();
        let mut rows: Vec<(i32, String, String)> = Vec::new();
        for b in &batches {
            let ids = arrow::compute::cast(b.column(0), &DataType::Int32).unwrap();
            let ids = ids.as_any().downcast_ref::<Int32Array>().unwrap();
            let ssn = arrow::compute::cast(b.column(1), &DataType::Utf8).unwrap();
            let ssn = ssn.as_any().downcast_ref::<StringArray>().unwrap();
            let note = arrow::compute::cast(b.column(2), &DataType::Utf8).unwrap();
            let note = note.as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..b.num_rows() {
                rows.push((
                    ids.value(i),
                    ssn.value(i).to_string(),
                    note.value(i).to_string(),
                ));
            }
        }
        rows.sort();
        assert_eq!(
            rows,
            vec![
                (1, "111".to_string(), keyed),
                (2, "222".to_string(), "b".to_string()),
            ],
            "SET reads the keyed mask; stored ssn and unmatched rows are kept"
        );

        // The function stays out of the user's session, also inside a MERGE
        // that reads the masked view and in a USING-style SELECT.
        assert!(
            !ctx.state()
                .scalar_functions()
                .contains_key(MASK_HASH_UDF_NAME),
            "mask UDF leaked into the session"
        );
        let user_call = plan_merge(&format!(
            "MERGE INTO tgt AS t USING src AS s ON t.id = s.sid \
             WHEN MATCHED THEN UPDATE SET note = {MASK_HASH_UDF_NAME}(note)"
        ));
        assert!(
            ctx.sql(&user_call).await.is_err(),
            "user SET resolved the mask function"
        );
        assert!(
            ctx.sql(&format!("SELECT {MASK_HASH_UDF_NAME}('111') AS sid"))
                .await
                .is_err(),
            "a USING query resolved the mask function"
        );
    }

    /// 501: the empty-source shortcut must sit *after* clause classification.
    ///
    /// Two halves. First, the shortcut's input is a `ClassifiedMerge`, so it
    /// can only be evaluated once `classify_merge_clauses` has run — a
    /// refactor that hoists the emptiness check above classification has
    /// nothing to pass it. Second, classification itself does not depend on
    /// the source having produced rows or columns, so there is no reason to
    /// bail early: a source with zero columns still classifies fine.
    #[test]
    fn empty_source_shortcut_requires_classification() {
        let tc = cols(&["id", "region"]);
        let empty_source: Vec<String> = Vec::new();
        let n = names(&tc, &empty_source);

        let by_source = merge_clauses(
            "MERGE INTO tgt AS t USING src AS s ON t.id = s.id \
             WHEN NOT MATCHED BY SOURCE THEN DELETE",
        );
        let classified = classify_merge_clauses(&by_source, &n).expect("classify");
        assert_eq!(classified.counts(), (0, 0, 1));
        assert!(
            !empty_source_is_no_op(&classified),
            "an empty source with a BY SOURCE clause deletes every target row"
        );

        let matched_only = merge_clauses(
            "MERGE INTO tgt AS t USING src AS s ON t.id = s.id \
             WHEN MATCHED THEN DELETE \
             WHEN NOT MATCHED THEN INSERT (id, region) VALUES (s.id, s.region)",
        );
        let classified = classify_merge_clauses(&matched_only, &n).expect("classify");
        assert_eq!(classified.counts(), (1, 1, 0));
        assert!(
            empty_source_is_no_op(&classified),
            "without a BY SOURCE clause an empty source changes nothing"
        );
    }

    /// 501, executed: an empty source plus `WHEN NOT MATCHED BY SOURCE THEN
    /// DELETE` empties the target (the merge SELECT the CoW handler writes
    /// back returns zero rows) and the affected-row count is the target's row
    /// count, not the number of rows written (which is zero).
    #[tokio::test]
    async fn empty_source_by_source_delete_empties_target_end_to_end() {
        use arrow_array::{Int32Array, StringArray};
        use arrow_schema::{DataType, Field, Schema};
        use datafusion::prelude::SessionContext;
        use std::sync::Arc;

        let ctx = SessionContext::new();
        let tschema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("region", DataType::Utf8, false),
        ]));
        let target = arrow_array::RecordBatch::try_new(
            tschema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["EU", "US", "US"])),
            ],
        )
        .unwrap();
        // The source produced no batches. Its schema comes from the source
        // plan (`source_schema_hint` in the handler), not from the data.
        let sschema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        ctx.register_table(
            "datafusion.public.__merge_target_x",
            Arc::new(
                datafusion::datasource::MemTable::try_new(tschema, vec![vec![target]]).unwrap(),
            ),
        )
        .unwrap();
        ctx.register_table(
            "datafusion.public.__merge_source_x",
            Arc::new(datafusion::datasource::MemTable::try_new(sschema, vec![vec![]]).unwrap()),
        )
        .unwrap();

        let tc = cols(&["id", "region"]);
        let sc = cols(&["id"]);
        let n = names(&tc, &sc);
        let clauses = merge_clauses(
            "MERGE INTO tgt AS t USING src AS s ON t.id = s.id \
             WHEN NOT MATCHED BY SOURCE THEN DELETE",
        );
        let classified = classify_merge_clauses(&clauses, &n).expect("classify");
        assert!(!empty_source_is_no_op(&classified));

        // The rows the CoW rewrite writes back: none, the table is emptied.
        let select = build_merge_select(
            &classified,
            &n,
            "datafusion.public.__merge_target_x",
            "datafusion.public.__merge_source_x",
            "__merge_target_x.id = __merge_source_x.id",
        );
        let batches = ctx.sql(&select).await.unwrap().collect().await.unwrap();
        let surviving: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(surviving, 0, "every target row is deleted: {select}");

        // ... and the reported count is the 3 rows it claimed.
        let count_sql =
            empty_source_claim_count_sql(&classified, &n, "datafusion.public.__merge_target_x")
                .expect("BY SOURCE clauses produce a count query");
        let counted = ctx.sql(&count_sql).await.unwrap().collect().await.unwrap();
        assert_eq!(
            counted[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .unwrap()
                .value(0),
            3,
            "rows_affected must be the 3 deleted target rows: {count_sql}"
        );

        // The same statement shape the MoR handler builds for its BY SOURCE
        // DELETE (anti-join against the empty source) claims the same 3 rows.
        let mor_sql = "SELECT __merge_target_x.\"id\" FROM datafusion.public.__merge_target_x \
                       WHERE NOT EXISTS (SELECT 1 FROM datafusion.public.__merge_source_x \
                       WHERE __merge_target_x.id = __merge_source_x.id) AND (TRUE)";
        let mor = ctx.sql(mor_sql).await.unwrap().collect().await.unwrap();
        let mor_rows: usize = mor.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            mor_rows, 3,
            "MoR BY SOURCE DELETE deletes all 3 target rows"
        );
    }

    /// The empty-source count honours per-clause predicates and the caller's
    /// row filter: only rows a BY SOURCE clause actually claims are counted,
    /// and rows the caller cannot see are out of scope (202).
    #[tokio::test]
    async fn empty_source_claim_count_honours_predicates_and_row_filter() {
        use arrow_array::{Int32Array, StringArray};
        use arrow_schema::{DataType, Field, Schema};
        use datafusion::prelude::SessionContext;
        use std::sync::Arc;

        let ctx = SessionContext::new();
        let tschema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("region", DataType::Utf8, false),
        ]));
        let target = arrow_array::RecordBatch::try_new(
            tschema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["EU", "US", "US"])),
            ],
        )
        .unwrap();
        ctx.register_table(
            "datafusion.public.__merge_target_x",
            Arc::new(
                datafusion::datasource::MemTable::try_new(tschema, vec![vec![target]]).unwrap(),
            ),
        )
        .unwrap();

        let tc = cols(&["id", "region"]);
        let sc = cols(&["id"]);
        let clauses = merge_clauses(
            "MERGE INTO tgt AS t USING src AS s ON t.id = s.id \
             WHEN NOT MATCHED BY SOURCE AND t.region = 'US' THEN DELETE",
        );

        let count = |policy: WritePolicyPredicates| {
            let ctx = ctx.clone();
            let tc = tc.clone();
            let sc = sc.clone();
            let clauses = clauses.clone();
            async move {
                let n = names_with_policy(&tc, &sc, &policy);
                let classified = classify_merge_clauses(&clauses, &n).expect("classify");
                let sql = empty_source_claim_count_sql(
                    &classified,
                    &n,
                    "datafusion.public.__merge_target_x",
                )
                .expect("BY SOURCE clause present");
                let out = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
                out[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .unwrap()
                    .value(0)
            }
        };

        assert_eq!(
            count(WritePolicyPredicates::default()).await,
            2,
            "only the two US rows are claimed by the predicated clause"
        );
        assert_eq!(
            count(WritePolicyPredicates {
                row_filter_sql: Some("region = 'EU'".to_string()),
                ..Default::default()
            })
            .await,
            0,
            "an EU-only caller claims no US row"
        );
    }

    /// Without a BY SOURCE clause there is nothing to count: the handler
    /// returns before committing, so no count query is built.
    #[test]
    fn empty_source_claim_count_is_none_without_by_source_clauses() {
        let tc = cols(&["id", "region"]);
        let sc = cols(&["id"]);
        let n = names(&tc, &sc);
        let clauses = merge_clauses(
            "MERGE INTO tgt AS t USING src AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET region = s.id",
        );
        let classified = classify_merge_clauses(&clauses, &n).expect("classify");
        assert!(empty_source_claim_count_sql(
            &classified,
            &n,
            "datafusion.public.__merge_target_x"
        )
        .is_none());
    }

    fn cols(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    fn build(sql: &str, target_cols: &[&str], source_cols: &[&str]) -> String {
        let clauses = merge_clauses(sql);
        let tc = cols(target_cols);
        let sc = cols(source_cols);
        let n = names(&tc, &sc);
        let classified = classify_merge_clauses(&clauses, &n).expect("classify");
        build_merge_select(
            &classified,
            &n,
            "datafusion.public.__merge_target_x",
            "datafusion.public.__merge_source_x",
            "__merge_target_x.id = __merge_source_x.id",
        )
    }

    #[test]
    fn scd2_snapshot_shape_builds_predicated_arms() {
        // The dbt snapshot MERGE shape: predicated matched update closing the
        // validity window plus a predicated insert for new versions.
        let sql = "MERGE INTO tgt AS t USING src AS s ON t.dbt_scd_id = s.dbt_scd_id \
                   WHEN MATCHED AND t.valid_to IS NULL AND s.change_type IN ('update', 'delete') \
                     THEN UPDATE SET valid_to = s.valid_to \
                   WHEN NOT MATCHED AND s.change_type = 'insert' \
                     THEN INSERT (dbt_scd_id, valid_to) VALUES (s.dbt_scd_id, s.valid_to)";
        let out = build(
            sql,
            &["dbt_scd_id", "valid_to"],
            &["dbt_scd_id", "valid_to", "change_type"],
        );

        // The matched predicate gates the update arm, rewritten to scratch refs.
        assert!(
            out.contains(
                "AND (__merge_target_x.valid_to IS NULL AND \
                 __merge_source_x.change_type IN ('update', 'delete'))"
            ),
            "matched predicate missing or not rewritten: {out}"
        );
        // The insert predicate gates the source-only arm.
        assert!(
            out.contains("AND (__merge_source_x.change_type = 'insert')"),
            "insert predicate missing: {out}"
        );
        // Deleted/unclaimed rows are dropped by the keep filter.
        assert!(
            out.ends_with(&format!("WHERE {MERGE_KEEP_COLUMN}")),
            "{out}"
        );
        // Source-only rows failing the insert predicate are not inserted.
        // Source-only is now detected via the target presence flag, not the
        // first data column.
        let t_flag = present_flag("__merge_target_x");
        assert!(
            out.contains(&format!(
                "WHEN __merge_target_x.\"{t_flag}\" IS NULL THEN FALSE"
            )),
            "source-only default drop arm missing: {out}"
        );
    }

    #[test]
    fn row_class_detection_uses_presence_flags_not_data_columns() {
        // #374: match class must not depend on any user column value. The
        // generated SQL wraps each side with a presence flag and gates the
        // classes on that flag, so a nullable first column can't misclassify.
        let sql = "MERGE INTO tgt AS t USING src AS s ON t.id = s.id \
                   WHEN MATCHED THEN UPDATE SET v = s.v \
                   WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v) \
                   WHEN NOT MATCHED BY SOURCE THEN DELETE";
        let out = build(sql, &["id", "v"], &["id", "v"]);
        let t_flag = present_flag("__merge_target_x");
        let s_flag = present_flag("__merge_source_x");
        // Both sides are wrapped in a derived table adding the flag.
        assert!(
            out.contains(&format!(
                "(SELECT *, TRUE AS \"{t_flag}\" FROM datafusion.public.__merge_target_x) AS __merge_target_x"
            )),
            "target presence-flag wrapping missing: {out}"
        );
        assert!(
            out.contains(&format!(
                "(SELECT *, TRUE AS \"{s_flag}\" FROM datafusion.public.__merge_source_x) AS __merge_source_x"
            )),
            "source presence-flag wrapping missing: {out}"
        );
        // Class conditions reference the flags, never the id/v data columns.
        assert!(
            out.contains(&format!("__merge_target_x.\"{t_flag}\" IS NOT NULL")),
            "matched cond not flag-based: {out}"
        );
        // The flag column is not projected out of the inner select or the outer
        // projection (outer selects only the target columns).
        assert!(
            out.starts_with("SELECT \"id\", \"v\" FROM"),
            "flag leaked into outer projection: {out}"
        );
    }

    #[test]
    fn matched_clause_order_is_first_match_wins() {
        let sql = "MERGE INTO tgt AS t USING src AS s ON t.id = s.id \
                   WHEN MATCHED AND s.op = 'delete' THEN DELETE \
                   WHEN MATCHED THEN UPDATE SET v = s.v";
        let out = build(sql, &["id", "v"], &["id", "v", "op"]);
        // In the keep CASE the predicated DELETE arm (FALSE) must precede the
        // unpredicated UPDATE arm (TRUE).
        let del = out
            .find("AND (__merge_source_x.op = 'delete') THEN FALSE")
            .expect("delete keep arm");
        let upd_true = out[del..].find("THEN TRUE").expect("update keep arm after");
        assert!(upd_true > 0);
    }

    #[test]
    fn not_matched_by_source_delete_drops_target_only_rows() {
        let sql = "MERGE INTO tgt AS t USING src AS s ON t.id = s.id \
                   WHEN MATCHED THEN UPDATE SET v = s.v \
                   WHEN NOT MATCHED BY SOURCE THEN DELETE";
        let out = build(sql, &["id", "v"], &["id", "v"]);
        let s_flag = present_flag("__merge_source_x");
        assert!(
            out.contains(&format!(
                "WHEN __merge_source_x.\"{s_flag}\" IS NULL THEN FALSE"
            )),
            "by-source delete keep arm missing: {out}"
        );
    }

    #[test]
    fn not_matched_by_source_update_applies_target_side_assignments() {
        let sql = "MERGE INTO tgt AS t USING src AS s ON t.id = s.id \
                   WHEN NOT MATCHED BY SOURCE AND t.active THEN UPDATE SET active = false";
        let out = build(sql, &["id", "active"], &["id"]);
        let s_flag = present_flag("__merge_source_x");
        assert!(
            out.contains(&format!(
                "WHEN __merge_source_x.\"{s_flag}\" IS NULL AND (__merge_target_x.active) THEN false"
            )),
            "by-source update arm missing: {out}"
        );
        // Rows failing the predicate pass through unchanged (ELSE TRUE keep).
        assert!(out.contains("ELSE TRUE END AS __sqe_merge_keep"), "{out}");
    }

    #[test]
    fn classify_accepts_plain_matched_update() {
        let clauses = merge_clauses(
            "MERGE INTO tgt AS t USING src AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET v = s.v",
        );
        let tc = cols(&["id", "v"]);
        let sc = cols(&["id", "v"]);
        let n = names(&tc, &sc);
        assert!(classify_merge_clauses(&clauses, &n).is_ok());
    }

    #[test]
    fn classify_rejects_invalid_clause_action_combination() {
        // Every invalid combination is rejected by sqlparser at parse time,
        // so mutate a parsed clause to reach classify's validation arm:
        // NOT MATCHED + DELETE is not a legal pairing.
        let mut clauses = merge_clauses(
            "MERGE INTO tgt AS t USING src AS s ON t.id = s.id \
             WHEN MATCHED THEN DELETE",
        );
        clauses[0].clause_kind = MergeClauseKind::NotMatched;
        let tc = cols(&["id"]);
        let sc = cols(&["id"]);
        let n = names(&tc, &sc);
        let Err(err) = classify_merge_clauses(&clauses, &n) else {
            panic!("NOT MATCHED + DELETE must be rejected");
        };
        assert!(
            err.to_string()
                .contains("Unsupported MERGE clause combination"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn classify_rejects_oracle_update_where_subpredicate() {
        let clauses = merge_clauses(
            "MERGE INTO tgt AS t USING src AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET v = s.v WHERE t.x > 0",
        );
        let tc = cols(&["id", "v"]);
        let sc = cols(&["id", "v", "x"]);
        let n = names(&tc, &sc);
        let Err(err) = classify_merge_clauses(&clauses, &n) else {
            panic!("Oracle UPDATE ... WHERE must be rejected");
        };
        assert!(
            err.to_string().contains("Oracle-style sub-predicates"),
            "{err}"
        );
    }

    #[test]
    fn classify_rejects_oracle_update_delete_where_subpredicate() {
        let clauses = merge_clauses(
            "MERGE INTO tgt AS t USING src AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET v = s.v DELETE WHERE t.y < 0",
        );
        let tc = cols(&["id", "v"]);
        let sc = cols(&["id", "v", "y"]);
        let n = names(&tc, &sc);
        assert!(
            classify_merge_clauses(&clauses, &n).is_err(),
            "Oracle DELETE WHERE must be rejected"
        );
    }

    #[test]
    fn classify_rejects_oracle_insert_where_subpredicate() {
        let clauses = merge_clauses(
            "MERGE INTO tgt AS t USING src AS s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v) WHERE s.z = 1",
        );
        let tc = cols(&["id", "v"]);
        let sc = cols(&["id", "v", "z"]);
        let n = names(&tc, &sc);
        let Err(err) = classify_merge_clauses(&clauses, &n) else {
            panic!("Oracle INSERT ... WHERE must be rejected");
        };
        assert!(err.to_string().contains("INSERT ... WHERE"), "{err}");
    }

    #[test]
    fn alias_rewrite_is_identifier_aware() {
        // Alias `s` must not fire inside `users.name` (substring), inside a
        // dotted path `a.s.x` (member access), or inside a string literal.
        let sql = "MERGE INTO tgt AS t USING src AS s ON t.id = s.id \
                   WHEN MATCHED AND users.name = s.name AND a.s.x = 's.literal' \
                     THEN UPDATE SET v = s.v";
        let out = build(sql, &["id", "v"], &["id", "v", "name"]);
        assert!(
            out.contains("users.name"),
            "substring alias misfired: {out}"
        );
        assert!(out.contains("a.s.x"), "dotted-path alias misfired: {out}");
        assert!(out.contains("'s.literal'"), "literal rewritten: {out}");
        assert!(
            out.contains("__merge_source_x.name"),
            "genuine alias not rewritten: {out}"
        );
    }

    #[test]
    fn replace_alias_qualifier_boundaries() {
        assert_eq!(
            replace_alias_qualifier("s.x + users.x + a.s.x + 's.x'", "s", "R"),
            "R.x + users.x + a.s.x + 's.x'"
        );
        assert_eq!(replace_alias_qualifier("(s.x)", "s", "R"), "(R.x)");
        assert_eq!(replace_alias_qualifier("_s.x", "s", "R"), "_s.x");
        assert_eq!(replace_alias_qualifier("\"s\".x", "s", "R"), "\"s\".x");
        assert_eq!(
            replace_alias_qualifier("'it''s.x' = s.y", "s", "R"),
            "'it''s.x' = R.y"
        );
    }

    #[test]
    fn first_match_guard_orders_predicates() {
        let clauses = merge_clauses(
            "MERGE INTO tgt AS t USING src AS s ON t.id = s.id \
             WHEN MATCHED AND s.op = 'del' THEN DELETE \
             WHEN MATCHED AND s.op = 'upd' THEN UPDATE SET v = s.v \
             WHEN MATCHED THEN UPDATE SET v = s.v",
        );
        let tc = cols(&["id", "v"]);
        let sc = cols(&["id", "v", "op"]);
        let n = names(&tc, &sc);
        let c = classify_merge_clauses(&clauses, &n).unwrap();
        assert_eq!(c.matched.len(), 3);
        assert_eq!(
            first_match_guard(&c.matched, 0),
            "COALESCE((__merge_source_x.op = 'del'), FALSE)"
        );
        assert_eq!(
            first_match_guard(&c.matched, 1),
            "COALESCE((__merge_source_x.op = 'upd'), FALSE) AND \
             NOT COALESCE((__merge_source_x.op = 'del'), FALSE)"
        );
        // Trailing unconditional clause: no own term, just NOT of both priors.
        assert_eq!(
            first_match_guard(&c.matched, 2),
            "NOT COALESCE((__merge_source_x.op = 'del'), FALSE) AND \
             NOT COALESCE((__merge_source_x.op = 'upd'), FALSE)"
        );
    }

    #[test]
    fn first_match_guard_after_unconditional_collapses_to_false() {
        let clauses = merge_clauses(
            "MERGE INTO tgt AS t USING src AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET v = s.v \
             WHEN MATCHED AND s.op = 'x' THEN DELETE",
        );
        let tc = cols(&["id", "v"]);
        let sc = cols(&["id", "v", "op"]);
        let n = names(&tc, &sc);
        let c = classify_merge_clauses(&clauses, &n).unwrap();
        assert_eq!(first_match_guard(&c.matched, 0), "TRUE");
        // The clause after an unconditional one is unreachable.
        assert!(
            first_match_guard(&c.matched, 1).contains("FALSE"),
            "{}",
            first_match_guard(&c.matched, 1)
        );
    }

    #[test]
    fn unclaimed_matched_rows_pass_through() {
        let sql = "MERGE INTO tgt AS t USING src AS s ON t.id = s.id \
                   WHEN MATCHED AND s.op = 'u' THEN UPDATE SET v = s.v";
        let out = build(sql, &["id", "v"], &["id", "v", "op"]);
        // Data CASE falls back to the target column.
        assert!(
            out.contains("ELSE __merge_target_x.\"v\" END AS \"v\""),
            "{out}"
        );
        // A matched row failing the predicate keeps (ELSE TRUE), it is not
        // deleted: the only FALSE arms are the source-only default.
        assert_eq!(out.matches("THEN FALSE").count(), 1, "{out}");
    }

    #[test]
    fn cardinality_check_sql_shape() {
        let tc = cols(&["id", "v"]);
        let sc = cols(&["id", "v"]);
        let n = names(&tc, &sc);
        let (pair, matched) = build_cardinality_check_sql(
            &n,
            "datafusion.public.__merge_target_x",
            "datafusion.public.__merge_source_x",
            "__merge_target_x.id = __merge_source_x.id",
        );
        assert!(
            pair.contains("JOIN datafusion.public.__merge_source_x AS __merge_source_x ON"),
            "pair count is a plain join: {pair}"
        );
        assert!(
            matched.contains("WHERE EXISTS (SELECT 1 FROM datafusion.public.__merge_source_x"),
            "matched count is an EXISTS semi-join: {matched}"
        );
    }

    // ---- In-process DataFusion execution tests ---------------------------
    // These actually run the generated SQL against MemTables, so they verify
    // row-level behaviour (not just SQL shape) without a live catalog.

    use arrow_array::{Array, Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    fn id_v_batch(ids: Vec<Option<i32>>, vs: Vec<Option<&str>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("v", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(vs)),
            ],
        )
        .unwrap()
    }

    async fn run_sql(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
        ctx.sql(sql)
            .await
            .expect("plan")
            .collect()
            .await
            .expect("collect")
    }

    /// Collect (id, v) rows from result batches, sorted by id for a stable
    /// assertion.
    fn id_v_rows(batches: &[RecordBatch]) -> Vec<(Option<i32>, Option<String>)> {
        let mut rows = Vec::new();
        for b in batches {
            let ids = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            let vs = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..b.num_rows() {
                let id = if ids.is_null(i) {
                    None
                } else {
                    Some(ids.value(i))
                };
                let v = if vs.is_null(i) {
                    None
                } else {
                    Some(vs.value(i).to_string())
                };
                rows.push((id, v));
            }
        }
        rows.sort();
        rows
    }

    #[tokio::test]
    async fn execute_first_match_wins_delete_before_update_and_insert() {
        // matched+DELETE (predicated) precedes matched+UPDATE; source-only
        // inserts; target-only passes through unchanged.
        let ctx = SessionContext::new();
        ctx.register_batch(
            "__merge_target_x",
            id_v_batch(
                vec![Some(1), Some(2), Some(3)],
                vec![Some("a"), Some("b"), Some("c")],
            ),
        )
        .unwrap();
        ctx.register_batch(
            "__merge_source_x",
            id_v_batch(
                vec![Some(2), Some(3), Some(4)],
                vec![Some("del"), Some("B"), Some("D")],
            ),
        )
        .unwrap();

        let sql = "MERGE INTO tgt AS t USING src AS s ON t.id = s.id \
                   WHEN MATCHED AND s.v = 'del' THEN DELETE \
                   WHEN MATCHED THEN UPDATE SET v = s.v \
                   WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)";
        let out = build(sql, &["id", "v"], &["id", "v"]);
        let rows = id_v_rows(&run_sql(&ctx, &out).await);
        // id=1 passthrough, id=2 deleted, id=3 updated to 'B', id=4 inserted.
        assert_eq!(
            rows,
            vec![
                (Some(1), Some("a".into())),
                (Some(3), Some("B".into())),
                (Some(4), Some("D".into())),
            ]
        );
    }

    #[tokio::test]
    async fn execute_null_first_column_still_classifies_as_matched() {
        // #374: a present target row whose FIRST column is NULL must still be
        // classified matched (via the presence flag), not misread as absent.
        // Column order puts the nullable non-key column first; join is on id.
        let schema = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Utf8, true),
            Field::new("id", DataType::Int32, true),
        ]));
        let tgt = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(Int32Array::from(vec![Some(2)])),
            ],
        )
        .unwrap();
        let src = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("new")])),
                Arc::new(Int32Array::from(vec![Some(2)])),
            ],
        )
        .unwrap();
        let ctx = SessionContext::new();
        ctx.register_batch("__merge_target_x", tgt).unwrap();
        ctx.register_batch("__merge_source_x", src).unwrap();

        let sql = "MERGE INTO tgt AS t USING src AS s ON t.id = s.id \
                   WHEN MATCHED THEN UPDATE SET v = s.v";
        let out = build(sql, &["v", "id"], &["v", "id"]);
        let batches = run_sql(&ctx, &out).await;
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1, "matched row must survive as one updated row");
        let vs = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            vs.value(0),
            "new",
            "matched row must be updated, not dropped/inserted"
        );
    }

    #[tokio::test]
    async fn execute_cardinality_check_detects_and_passes() {
        let tc = cols(&["id", "v"]);
        let sc = cols(&["id", "v"]);
        let n = names(&tc, &sc);
        let (pair_sql, matched_sql) = build_cardinality_check_sql(
            &n,
            "datafusion.public.__merge_target_x",
            "datafusion.public.__merge_source_x",
            "__merge_target_x.id = __merge_source_x.id",
        );
        let count = |batches: Vec<RecordBatch>| -> i64 {
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        };

        // Duplicate source key 1 -> target row 1 matches twice -> violation.
        let ctx = SessionContext::new();
        ctx.register_batch(
            "__merge_target_x",
            id_v_batch(vec![Some(1)], vec![Some("a")]),
        )
        .unwrap();
        ctx.register_batch(
            "__merge_source_x",
            id_v_batch(vec![Some(1), Some(1)], vec![Some("x"), Some("y")]),
        )
        .unwrap();
        let pair = count(run_sql(&ctx, &pair_sql).await);
        let matched = count(run_sql(&ctx, &matched_sql).await);
        assert!(
            pair > matched,
            "duplicate source key must violate: {pair} vs {matched}"
        );

        // Unique source keys -> no violation.
        let ctx2 = SessionContext::new();
        ctx2.register_batch(
            "__merge_target_x",
            id_v_batch(vec![Some(1)], vec![Some("a")]),
        )
        .unwrap();
        ctx2.register_batch(
            "__merge_source_x",
            id_v_batch(vec![Some(1), Some(2)], vec![Some("x"), Some("y")]),
        )
        .unwrap();
        let pair2 = count(run_sql(&ctx2, &pair_sql).await);
        let matched2 = count(run_sql(&ctx2, &matched_sql).await);
        assert!(
            pair2 <= matched2,
            "unique keys must not violate: {pair2} vs {matched2}"
        );
    }

    use arrow_array::Int64Array;
}
