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

use sqlparser::ast::{
    Assignment, MergeAction, MergeClause, MergeClauseKind, MergeInsertKind, ObjectName,
};
use sqe_core::SqeError;

/// Name of the synthetic boolean column that marks rows surviving the MERGE.
/// It exists only inside the merge SELECT; the outer projection drops it.
pub(crate) const MERGE_KEEP_COLUMN: &str = "__sqe_merge_keep";

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

/// True when the statement uses clause shapes the merge-on-read equality
/// path cannot express: clause predicates, `NOT MATCHED BY SOURCE`, or more
/// than one clause per row class (which needs first-match-wins ordering).
/// The dispatcher falls back to copy-on-write for these.
pub(crate) fn merge_needs_cow(clauses: &[MergeClause]) -> bool {
    let mut matched = 0usize;
    let mut not_matched = 0usize;
    for clause in clauses {
        if clause.predicate.is_some() {
            return true;
        }
        match clause.clause_kind {
            MergeClauseKind::NotMatchedBySource => return true,
            MergeClauseKind::Matched => matched += 1,
            MergeClauseKind::NotMatched | MergeClauseKind::NotMatchedByTarget => not_matched += 1,
        }
    }
    matched > 1 || not_matched > 1
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
    let t_flag = present_flag(names.target_ref);
    let s_flag = present_flag(names.source_ref);
    let target_sentinel = format!("{}.\"{t_flag}\"", names.target_ref);
    let source_sentinel = format!("{}.\"{s_flag}\"", names.source_ref);
    let matched_cond = format!("{target_sentinel} IS NOT NULL AND {source_sentinel} IS NOT NULL");
    let source_only_cond = format!("{target_sentinel} IS NULL");
    let target_only_cond = format!("{source_sentinel} IS NULL");

    let mut inner_cols: Vec<String> = Vec::with_capacity(names.target_columns.len() + 1);
    for col in names.target_columns {
        let passthrough = format!("{}.\"{col}\"", names.target_ref);
        let mut arms: Vec<String> = Vec::new();
        for clause in &classified.matched {
            // DELETE rows are dropped by the keep filter; project the target
            // column so every arm of the CASE keeps the column's type.
            let value = match &clause.op {
                MergeOp::Update(assignments) => resolve_update_expr(
                    col,
                    assignments,
                    names.target_ref,
                    names.source_ref,
                    names.t_alias,
                    names.s_alias,
                ),
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
                MergeOp::Update(assignments) => resolve_update_expr(
                    col,
                    assignments,
                    names.target_ref,
                    names.source_ref,
                    names.t_alias,
                    names.s_alias,
                ),
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

    let outer_projection = names
        .target_columns
        .iter()
        .map(|c| format!("\"{c}\""))
        .collect::<Vec<_>>()
        .join(", ");

    // Wrap each side in a derived table that adds the presence flag, so match
    // class depends on the flag rather than a nullable user column. The derived
    // tables keep the scratch-ref aliases, so the ON condition, passthrough,
    // and assignment references (`{ref}."col"`) all still resolve; the flag
    // columns stay inside the join scope and never reach the outer projection.
    format!(
        "SELECT {outer_projection} FROM (SELECT {} FROM \
         (SELECT *, TRUE AS \"{t_flag}\" FROM {qualified_target_ref}) AS {} \
         FULL OUTER JOIN \
         (SELECT *, TRUE AS \"{s_flag}\" FROM {qualified_source_ref}) AS {} \
         ON {on_rewritten}) \
         WHERE {MERGE_KEEP_COLUMN}",
        inner_cols.join(", "),
        names.target_ref,
        names.source_ref,
    )
}

/// Resolve the UPDATE expression for one target column from the SET
/// assignments, rewriting alias references to the scratch table names. A
/// column without an assignment passes through from the target.
pub(crate) fn resolve_update_expr(
    col: &str,
    assignments: &[Assignment],
    target_table_ref: &str,
    source_table_ref: &str,
    t_alias: &str,
    s_alias: &str,
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
                    let parts: Vec<String> = n
                        .0
                        .iter()
                        .filter_map(|p| p.as_ident())
                        .map(|i| i.value.clone())
                        .collect();
                    parts.last().cloned().unwrap_or_default()
                })
                .unwrap_or_default(),
        };
        if col_name == col {
            let expr_sql = format!("{}", a.value);
            // Rewrite alias references to MemTable names
            let out = replace_alias_qualifier(&expr_sql, t_alias, target_table_ref);
            return replace_alias_qualifier(&out, s_alias, source_table_ref);
        }
    }
    // Column not in SET assignments — pass through from target
    format!("{target_table_ref}.\"{col}\"")
}

/// Resolve the INSERT expression for one target column in the MERGE context.
///
/// Maps the INSERT column list + VALUES to find the expression for the
/// given target column. Rewrites alias references (e.g., `s.col`) to
/// use the MemTable name.
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
                    return "NULL".to_string();
                }
                "NULL".to_string()
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
                "NULL".to_string()
            }
        }
        MergeInsertKind::Row => {
            // INSERT ROW: use the source column with the same name
            if source_columns.contains(&col.to_string()) {
                format!("{source_table_ref}.\"{col}\"")
            } else {
                "NULL".to_string()
            }
        }
    }
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
        MergeNames {
            target_ref: "__merge_target_x",
            source_ref: "__merge_source_x",
            t_alias: "t",
            s_alias: "s",
            target_columns: target_cols,
            source_columns: source_cols,
        }
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
        let out = build(sql, &["dbt_scd_id", "valid_to"], &["dbt_scd_id", "valid_to", "change_type"]);

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
        assert!(out.ends_with(&format!("WHERE {MERGE_KEEP_COLUMN}")), "{out}");
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
            err.to_string().contains("Unsupported MERGE clause combination"),
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
        assert!(err.to_string().contains("Oracle-style sub-predicates"), "{err}");
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
        assert!(out.contains("users.name"), "substring alias misfired: {out}");
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
    fn merge_needs_cow_matrix() {
        let plain = merge_clauses(
            "MERGE INTO tgt AS t USING src AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET v = s.v \
             WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)",
        );
        assert!(!merge_needs_cow(&plain), "plain upsert stays MoR-eligible");

        let predicated = merge_clauses(
            "MERGE INTO tgt AS t USING src AS s ON t.id = s.id \
             WHEN MATCHED AND s.op = 'u' THEN UPDATE SET v = s.v",
        );
        assert!(merge_needs_cow(&predicated), "predicate forces CoW");

        let by_source = merge_clauses(
            "MERGE INTO tgt AS t USING src AS s ON t.id = s.id \
             WHEN NOT MATCHED BY SOURCE THEN DELETE",
        );
        assert!(merge_needs_cow(&by_source), "BY SOURCE forces CoW");

        let multi = merge_clauses(
            "MERGE INTO tgt AS t USING src AS s ON t.id = s.id \
             WHEN MATCHED THEN DELETE \
             WHEN MATCHED THEN UPDATE SET v = s.v",
        );
        assert!(merge_needs_cow(&multi), "multiple matched clauses force CoW");
    }

    #[test]
    fn unclaimed_matched_rows_pass_through() {
        let sql = "MERGE INTO tgt AS t USING src AS s ON t.id = s.id \
                   WHEN MATCHED AND s.op = 'u' THEN UPDATE SET v = s.v";
        let out = build(sql, &["id", "v"], &["id", "v", "op"]);
        // Data CASE falls back to the target column.
        assert!(out.contains("ELSE __merge_target_x.\"v\" END AS \"v\""), "{out}");
        // A matched row failing the predicate keeps (ELSE TRUE), it is not
        // deleted: the only FALSE arms are the source-only default.
        assert_eq!(out.matches("THEN FALSE").count(), 1, "{out}");
    }
}
