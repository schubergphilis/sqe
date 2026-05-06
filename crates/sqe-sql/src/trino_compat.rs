//! Trino-compat AST rewrites that don't fit cleanly into a UDF.
//!
//! These rewrites operate on the parsed sqlparser AST before SQL is
//! handed to DataFusion. Each rewrite is a small, well-scoped fix for a
//! Trino syntax that DataFusion does not natively recognize.
//!
//! Currently:
//!
//! - `CAST(v AS JSON)` -> `to_json(v)`. Trino's JSON cast serializes a
//!   value to a JSON-formatted string; SQE already has a `to_json(v)` UDF
//!   that does exactly that. DataFusion's SQL planner does not recognize
//!   `JSON` as a target type for CAST, so without this rewrite users get
//!   `Error: Unsupported SQL type JSON` at planning time.
//!
//! - `SELECT * FROM "ns.t$snapshots"` -> `SELECT * FROM table_snapshots('ns', 't')`.
//!   Trino exposes Iceberg metadata tables as virtual tables with a `$`
//!   suffix on the table name (`$snapshots`, `$manifests`, `$history`,
//!   `$partitions`, `$files`, `$refs`). SQE already exposes the same data
//!   through TVF calls registered in `sqe-catalog::iceberg_metadata_tvf`;
//!   this rewriter translates the Trino-spelled FROM clause to the TVF
//!   call so `dbt-trino` macros that hard-code `$snapshots` work without
//!   modification.
//!
//! `CAST(json_col AS T)` (the inverse direction) is intentionally NOT
//! rewritten here. SQE represents JSON columns as `Utf8`, and DataFusion's
//! built-in `Utf8 -> T` coercion already parses numeric / boolean strings
//! into the target type. Users who need typed JSONPath extraction should
//! call `json_get_int(col, '$')`, `json_get_str(col, '$')`, etc. directly.
//!
//! The rewrite preserves the SQL string when no rewrite-eligible pattern
//! is present, so the cost is one parse + one serialize for every query
//! that hits a rewriter. Errors during parse fall through silently (the
//! rewriter returns the original SQL); DataFusion will surface the same
//! parse error itself.

use std::ops::ControlFlow;

use sqlparser::ast::{
    DataType as SqlDataType, Expr, Function, FunctionArg, FunctionArgExpr,
    FunctionArgumentList, FunctionArguments, Ident, ObjectName, Statement, TableFactor,
    TableFunctionArgs, Value, VisitMut, VisitorMut,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Iceberg metadata-table suffixes Trino exposes via the `$<kind>` syntax
/// on a table name. Each maps to an SQE TVF registered in
/// `sqe-catalog::iceberg_metadata_tvf`. Listed lowercase; the rewriter
/// matches case-insensitively.
const METADATA_SUFFIXES: &[(&str, &str)] = &[
    ("snapshots", "table_snapshots"),
    ("manifests", "table_manifests"),
    ("history", "table_history"),
    ("partitions", "table_partitions"),
    ("files", "table_files"),
    ("refs", "table_refs"),
];

/// Rewrite Trino-compat AST patterns in `sql` that DataFusion does not
/// natively recognize. Returns the rewritten SQL as a new string. If the
/// input does not parse, returns the original string unchanged so the
/// downstream planner produces its own error message.
pub fn rewrite_trino_compat(sql: &str) -> String {
    let dialect = GenericDialect {};
    let mut statements = match Parser::parse_sql(&dialect, sql) {
        Ok(s) => s,
        Err(_) => return sql.to_string(),
    };

    // Cheap fast path: if the lowercased text contains neither `as json`
    // nor `$` nor any metadata suffix, no rewriter can fire. Skip the AST
    // walk and the re-serialization, which together cost more than the
    // substring check on the typical SQL string.
    let lower = sql.to_ascii_lowercase();
    let has_json_cast = lower.contains("as json");
    let has_dollar = lower.contains('$');
    if !has_json_cast && !has_dollar {
        return sql.to_string();
    }

    let mut visitor = TrinoCompatVisitor::default();
    let _ = statements.visit(&mut visitor);

    if visitor.rewrites == 0 {
        // Parsed cleanly but no rewrite fired. Avoid the round-trip
        // through Display: some SQL constructs do not round-trip exactly,
        // so leaving the original string alone preserves user formatting.
        return sql.to_string();
    }

    // Re-serialize each statement and join with `; `. sqlparser's Display
    // impl emits semicolon-separated statements without a trailing
    // semicolon, matching what `ctx.sql()` accepts.
    statements
        .iter()
        .map(Statement::to_string)
        .collect::<Vec<_>>()
        .join("; ")
}

/// Combined visitor that runs every Trino-compat rewriter. One walk over
/// the AST, rewrites accumulate into the `rewrites` counter so the caller
/// knows whether to re-serialize.
#[derive(Default)]
struct TrinoCompatVisitor {
    rewrites: usize,
}

impl VisitorMut for TrinoCompatVisitor {
    type Break = ();

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if rewrite_cast_as_json(expr) {
            self.rewrites += 1;
        }
        ControlFlow::Continue(())
    }

    fn post_visit_table_factor(
        &mut self,
        factor: &mut TableFactor,
    ) -> ControlFlow<Self::Break> {
        if rewrite_metadata_dollar_table(factor) {
            self.rewrites += 1;
        }
        ControlFlow::Continue(())
    }
}

/// Rewrite `Expr::Cast { data_type: JSON, expr }` to `to_json(expr)`.
/// Returns true if the rewrite fired.
fn rewrite_cast_as_json(expr: &mut Expr) -> bool {
    let Expr::Cast {
        kind: _,
        expr: inner,
        data_type,
        format: _,
    } = expr
    else {
        return false;
    };
    if !matches!(data_type, SqlDataType::JSON) {
        return false;
    }
    let inner_expr = std::mem::replace(
        inner.as_mut(),
        Expr::Identifier(Ident::new("__cast_as_json_placeholder__")),
    );
    *expr = Expr::Function(Function {
        name: ObjectName(vec![Ident::new("to_json")]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(inner_expr))],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    });
    true
}

/// Rewrite a `TableFactor::Table` whose name ends in `$<metadata_kind>`
/// (Trino's Iceberg metadata-table syntax) into a TVF call.
///
/// Examples:
///
/// - `FROM "ns.t$snapshots"` -> `FROM table_snapshots('ns', 't')`
/// - `FROM ns."t$manifests"` -> `FROM table_manifests('ns', 't')`
/// - `FROM "ns"."t$history"` -> `FROM table_history('ns', 't')`
///
/// The Trino quoted-identifier may collapse the namespace and the
/// `<table>$<kind>` part into a single identifier (when the user wrote
/// `"ns.t$snapshots"`) or split them across two idents (when each part
/// is quoted separately). We handle both shapes.
///
/// Returns true if the rewrite fired.
fn rewrite_metadata_dollar_table(factor: &mut TableFactor) -> bool {
    let TableFactor::Table {
        name,
        alias: _,
        args,
        version: _,
        ..
    } = factor
    else {
        return false;
    };

    // Already a TVF call? Don't double-rewrite.
    if args.is_some() {
        return false;
    }

    // The metadata suffix is on the LAST identifier component. Find it,
    // pull the suffix, and split into (bare_table, kind).
    let parts = &name.0;
    let Some(last) = parts.last() else {
        return false;
    };
    let last_str = last.value.as_str();
    let dollar_pos = last_str.rfind('$');
    let Some(dollar_pos) = dollar_pos else {
        return false;
    };
    let bare_table_in_last = &last_str[..dollar_pos];
    let suffix = &last_str[dollar_pos + 1..];

    // Match the suffix case-insensitively against the known kinds.
    let tvf_name = METADATA_SUFFIXES
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(suffix))
        .map(|(_, tvf)| *tvf);
    let Some(tvf_name) = tvf_name else {
        return false;
    };

    // Extract namespace and table name. Two cases:
    //   1) Single-ident form: `"ns.tbl$snapshots"` -> last_str contains
    //      a `.` before the `$`. Split on the last `.`.
    //   2) Multi-ident form: `"ns"."tbl$snapshots"` -> parts.len() >= 2,
    //      take parts[0..len-1].join(".") as the namespace.
    let (namespace, table_name): (String, String) = if parts.len() >= 2 {
        let ns = parts[..parts.len() - 1]
            .iter()
            .map(|i| i.value.clone())
            .collect::<Vec<_>>()
            .join(".");
        (ns, bare_table_in_last.to_string())
    } else if let Some(dot_in_last) = bare_table_in_last.rfind('.') {
        let ns = &bare_table_in_last[..dot_in_last];
        let tbl = &bare_table_in_last[dot_in_last + 1..];
        (ns.to_string(), tbl.to_string())
    } else {
        // Single-segment `"tbl$snapshots"` with no namespace. The TVFs
        // require both args, so we cannot rewrite without inventing a
        // namespace. Leave the FROM clause alone; DataFusion will produce
        // a "table not found" error which is the correct behaviour.
        return false;
    };

    // Build the TableFunctionArgs (two string-literal args).
    let args_vec = vec![
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
            Value::SingleQuotedString(namespace),
        ))),
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
            Value::SingleQuotedString(table_name),
        ))),
    ];

    // Replace the table name and attach args.
    *name = ObjectName(vec![Ident::new(tvf_name)]);
    *args = Some(TableFunctionArgs {
        args: args_vec,
        settings: None,
    });

    true
}

// (Per-rewriter logic now lives in `rewrite_cast_as_json` /
// `rewrite_metadata_dollar_table` above. The combined `TrinoCompatVisitor`
// dispatches both in one walk over the AST.)

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cast_as_json_rewritten_to_to_json() {
        let out = rewrite_trino_compat("SELECT CAST(123 AS JSON)");
        let lower = out.to_ascii_lowercase();
        assert!(
            lower.contains("to_json"),
            "expected to_json in rewritten SQL, got: {out}"
        );
        assert!(
            !lower.contains("as json"),
            "expected `AS JSON` to be removed, got: {out}"
        );
    }

    #[test]
    fn cast_as_json_with_complex_expr() {
        // The expression inside the CAST should round-trip intact.
        let out = rewrite_trino_compat("SELECT CAST(a + b * 2 AS JSON) FROM t");
        let lower = out.to_ascii_lowercase();
        assert!(lower.contains("to_json"));
        assert!(out.contains("a + b * 2"), "operands should survive: {out}");
    }

    #[test]
    fn nested_cast_as_json() {
        // Nested CASTs: outer is AS JSON, inner is AS BIGINT.
        let out = rewrite_trino_compat("SELECT CAST(CAST(x AS BIGINT) AS JSON) FROM t");
        let lower = out.to_ascii_lowercase();
        assert!(lower.contains("to_json"), "outer cast missing: {out}");
        assert!(lower.contains("cast"), "inner cast removed: {out}");
        assert!(lower.contains("bigint"), "BIGINT type removed: {out}");
    }

    #[test]
    fn no_cast_returns_input_unchanged() {
        let sql = "SELECT a, b FROM t WHERE c > 5";
        let out = rewrite_trino_compat(sql);
        assert_eq!(out, sql);
    }

    #[test]
    fn unparseable_input_returns_input_unchanged() {
        // Not even valid SQL; the rewriter must not panic and should
        // return the input verbatim so DataFusion produces its own error.
        let sql = "this is not SQL { @ }";
        let out = rewrite_trino_compat(sql);
        assert_eq!(out, sql);
    }

    #[test]
    fn other_cast_types_unchanged() {
        // Only AS JSON is rewritten. CAST AS BIGINT etc. must stay intact.
        let out = rewrite_trino_compat("SELECT CAST('123' AS BIGINT)");
        assert!(
            out.to_ascii_lowercase().contains("cast")
                && out.to_ascii_lowercase().contains("bigint"),
            "non-JSON CAST should be preserved: {out}"
        );
    }

    #[test]
    fn cast_as_json_in_where_clause() {
        let out = rewrite_trino_compat(
            "SELECT 1 FROM t WHERE CAST(payload AS JSON) IS NOT NULL",
        );
        assert!(
            out.to_ascii_lowercase().contains("to_json"),
            "WHERE-clause CAST should be rewritten: {out}"
        );
    }

    // ─── Metadata $-syntax rewriter ────────────────────────────────────────

    #[test]
    fn dollar_snapshots_single_quoted_full_name() {
        // Full name in one quoted ident: "ns.t$snapshots"
        let out = rewrite_trino_compat(r#"SELECT * FROM "ns.t$snapshots""#);
        let lower = out.to_ascii_lowercase();
        assert!(
            lower.contains("table_snapshots('ns', 't')"),
            "expected TVF call, got: {out}"
        );
        assert!(
            !lower.contains("$snapshots"),
            "$snapshots literal should be gone: {out}"
        );
    }

    #[test]
    fn dollar_snapshots_split_quoted_idents() {
        // Two quoted parts: "ns"."t$snapshots"
        let out = rewrite_trino_compat(r#"SELECT * FROM "ns"."t$snapshots""#);
        let lower = out.to_ascii_lowercase();
        assert!(
            lower.contains("table_snapshots('ns', 't')"),
            "expected TVF call, got: {out}"
        );
    }

    #[test]
    fn dollar_manifests_history_partitions_files_refs() {
        // Each suffix maps to a different TVF.
        let cases = [
            ("manifests", "table_manifests"),
            ("history", "table_history"),
            ("partitions", "table_partitions"),
            ("files", "table_files"),
            ("refs", "table_refs"),
        ];
        for (suffix, tvf) in cases {
            let sql = format!(r#"SELECT * FROM "ns.t${suffix}""#);
            let out = rewrite_trino_compat(&sql);
            let lower = out.to_ascii_lowercase();
            assert!(
                lower.contains(&format!("{tvf}('ns', 't')")),
                "expected {tvf}('ns', 't') for ${suffix}, got: {out}"
            );
        }
    }

    #[test]
    fn dollar_suffix_case_insensitive() {
        // Uppercase suffix should still match.
        let out = rewrite_trino_compat(r#"SELECT * FROM "ns.t$SNAPSHOTS""#);
        assert!(
            out.to_ascii_lowercase().contains("table_snapshots('ns', 't')"),
            "uppercase suffix should match, got: {out}"
        );
    }

    #[test]
    fn unknown_dollar_suffix_left_alone() {
        // `$wat` is not a known metadata table; the rewriter must not fire.
        // sqlparser may or may not parse this; either way the result must
        // not contain a TVF call.
        let out = rewrite_trino_compat(r#"SELECT * FROM "ns.t$wat""#);
        assert!(
            !out.to_ascii_lowercase().contains("table_wat("),
            "unknown $suffix should be left alone, got: {out}"
        );
    }

    #[test]
    fn dollar_in_normal_table_name_left_alone() {
        // No $<known_kind> suffix means no rewrite. `t$snapshots_archive`
        // does not match the suffix list and must be preserved.
        let out = rewrite_trino_compat(r#"SELECT * FROM "ns.t$snapshots_archive""#);
        assert!(
            !out.to_ascii_lowercase().contains("table_snapshots("),
            "non-suffix dollar should not trigger rewrite: {out}"
        );
    }

    #[test]
    fn no_namespace_dollar_table_left_alone() {
        // Single-segment `"t$snapshots"` cannot become a TVF call (no ns).
        // Leave it alone so DataFusion produces a table-not-found error.
        let out = rewrite_trino_compat(r#"SELECT * FROM "t$snapshots""#);
        assert!(
            !out.to_ascii_lowercase().contains("table_snapshots("),
            "single-segment $-table should not be rewritten: {out}"
        );
    }

    #[test]
    fn dollar_table_with_alias() {
        // Aliasing the metadata table should be preserved through rewrite.
        let out = rewrite_trino_compat(
            r#"SELECT s.committed_at FROM "ns.t$snapshots" AS s"#,
        );
        let lower = out.to_ascii_lowercase();
        assert!(
            lower.contains("table_snapshots('ns', 't')"),
            "expected TVF call, got: {out}"
        );
        // sqlparser preserves the AS alias through the rewrite.
        assert!(
            lower.contains(" s") || lower.contains("as s"),
            "alias should survive: {out}"
        );
    }

    #[test]
    fn dollar_table_combined_with_cast_as_json() {
        // Both rewriters should fire in one query.
        let out = rewrite_trino_compat(
            r#"SELECT CAST(snapshot_id AS JSON) FROM "ns.t$snapshots""#,
        );
        let lower = out.to_ascii_lowercase();
        assert!(lower.contains("to_json"), "JSON cast missing: {out}");
        assert!(
            lower.contains("table_snapshots('ns', 't')"),
            "TVF call missing: {out}"
        );
    }

    #[test]
    fn dollar_table_three_segment_namespace() {
        // catalog.schema.table format: three quoted parts.
        let out = rewrite_trino_compat(
            r#"SELECT * FROM "cat"."schema"."t$snapshots""#,
        );
        let lower = out.to_ascii_lowercase();
        // Namespace becomes "cat.schema"; the TVF takes the joined ns + bare table.
        assert!(
            lower.contains("table_snapshots('cat.schema', 't')"),
            "three-segment ns should join, got: {out}"
        );
    }
}
