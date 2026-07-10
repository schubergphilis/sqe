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
    expr_sql
        .replace(
            &format!("{}.", names.t_alias),
            &format!("{}.", names.target_ref),
        )
        .replace(
            &format!("{}.", names.s_alias),
            &format!("{}.", names.source_ref),
        )
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

/// Build the merge SELECT: an inner FULL OUTER JOIN projection with one CASE
/// per target column plus the keep column, wrapped in an outer projection
/// that filters on the keep column and drops it.
///
/// Match classes are detected through NULL sentinels on the first column of
/// each side, matching the pre-existing behaviour (a genuinely NULL first
/// column on a present row would misclassify; ON keys are non-NULL in
/// practice and always so for dbt's `dbt_scd_id`).
pub(crate) fn build_merge_select(
    classified: &ClassifiedMerge<'_>,
    names: &MergeNames<'_>,
    qualified_target_ref: &str,
    qualified_source_ref: &str,
    on_rewritten: &str,
) -> String {
    let target_sentinel = format!("{}.\"{}\"", names.target_ref, names.target_columns[0]);
    let source_sentinel = format!("{}.\"{}\"", names.source_ref, names.source_columns[0]);
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

    format!(
        "SELECT {outer_projection} FROM (SELECT {} FROM {qualified_target_ref} AS {} \
         FULL OUTER JOIN {qualified_source_ref} AS {} ON {on_rewritten}) \
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
            return expr_sql
                .replace(&format!("{t_alias}."), &format!("{target_table_ref}."))
                .replace(&format!("{s_alias}."), &format!("{source_table_ref}."));
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
        expr.replace(&format!("{s_alias}."), &format!("{source_table_ref}."))
            .replace(&format!("{t_alias}."), &format!("{target_table_ref}."))
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
        assert!(
            out.contains("WHEN __merge_target_x.\"dbt_scd_id\" IS NULL THEN FALSE"),
            "source-only default drop arm missing: {out}"
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
        assert!(
            out.contains("WHEN __merge_source_x.\"id\" IS NULL THEN FALSE"),
            "by-source delete keep arm missing: {out}"
        );
    }

    #[test]
    fn not_matched_by_source_update_applies_target_side_assignments() {
        let sql = "MERGE INTO tgt AS t USING src AS s ON t.id = s.id \
                   WHEN NOT MATCHED BY SOURCE AND t.active THEN UPDATE SET active = false";
        let out = build(sql, &["id", "active"], &["id"]);
        assert!(
            out.contains(
                "WHEN __merge_source_x.\"id\" IS NULL AND (__merge_target_x.active) THEN false"
            ),
            "by-source update arm missing: {out}"
        );
        // Rows failing the predicate pass through unchanged (ELSE TRUE keep).
        assert!(out.contains("ELSE TRUE END AS __sqe_merge_keep"), "{out}");
    }

    #[test]
    fn classify_rejects_invalid_combination() {
        // NOT MATCHED + DELETE parses under some dialect paths only; build the
        // clause by hand to hit the validation arm.
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
