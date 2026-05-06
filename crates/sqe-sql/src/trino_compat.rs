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
//! `CAST(json_col AS T)` (the inverse direction) is intentionally NOT
//! rewritten here. SQE represents JSON columns as `Utf8`, and DataFusion's
//! built-in `Utf8 -> T` coercion already parses numeric / boolean strings
//! into the target type. Users who need typed JSONPath extraction should
//! call `json_get_int(col, '$')`, `json_get_str(col, '$')`, etc. directly.
//!
//! The rewrite preserves the SQL string when no `CAST(... AS JSON)` is
//! present, so the cost is one parse + one serialize for every query.
//! Errors during parse fall through silently (the rewriter returns the
//! original SQL); DataFusion will surface the same parse error itself.

use std::ops::ControlFlow;

use sqlparser::ast::{
    DataType as SqlDataType, Expr, Function, FunctionArg, FunctionArgExpr,
    FunctionArgumentList, FunctionArguments, Ident, ObjectName, Statement, VisitMut, VisitorMut,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

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

    // Cheap fast path: if the lowercased text never mentions `as json`,
    // the visitor cannot find anything to rewrite. Skip the AST walk and
    // the re-serialization, which together cost more than the substring
    // check on the typical SQL string.
    if !sql.to_ascii_lowercase().contains("as json") {
        return sql.to_string();
    }

    let mut visitor = JsonCastRewriter { rewrites: 0 };
    let _ = statements.visit(&mut visitor);

    if visitor.rewrites == 0 {
        // Parsed cleanly but no `CAST(... AS JSON)` to rewrite. Avoid the
        // round-trip-through-Display cost. Some SQL constructs do not
        // round-trip exactly through sqlparser's Display impl, so leaving
        // the original string alone preserves user formatting.
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

/// VisitorMut that rewrites `Expr::Cast { data_type: JSON, expr }` into
/// a `to_json(expr)` function call.
struct JsonCastRewriter {
    rewrites: usize,
}

impl VisitorMut for JsonCastRewriter {
    type Break = ();

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if let Expr::Cast {
            kind: _,
            expr: inner,
            data_type,
            format: _,
        } = expr
        {
            if matches!(data_type, SqlDataType::JSON) {
                let inner_expr = std::mem::replace(
                    inner.as_mut(),
                    // Sentinel value; sqlparser requires a valid Expr to
                    // swap out. Identifier(__cast_as_json_placeholder__)
                    // would never appear in real SQL and we replace the
                    // outer expression in the next line anyway.
                    Expr::Identifier(Ident::new("__cast_as_json_placeholder__")),
                );
                let to_json = Expr::Function(Function {
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
                *expr = to_json;
                self.rewrites += 1;
            }
        }
        ControlFlow::Continue(())
    }
}

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
}
