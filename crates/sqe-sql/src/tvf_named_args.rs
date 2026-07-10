//! AST rewrite that converts named TVF arguments to positional `'key=value'`
//! string literals.
//!
//! DataFusion 54 tightened its handling of `TableFactor` args: it now rejects
//! `FunctionArg::Named` outright, and the `name = value` binary-expression
//! form fails because the identifier on the left is resolved against an empty
//! schema.  Both forms appear in user SQL today:
//!
//! ```sql
//! -- Arrow / named form (sqlparser -> FunctionArg::Named)
//! SELECT * FROM read_parquet('s3://b/x.parquet', access_key => 'AKIA', region => 'eu')
//!
//! -- Equals / binary-expression form (sqlparser -> FunctionArg::Unnamed(BinaryExpr))
//! SELECT * FROM read_csv('/x.csv', delimiter = ';')
//! ```
//!
//! Both are rewritten to positional single-quoted string literals:
//!
//! ```sql
//! SELECT * FROM read_parquet('s3://b/x.parquet', 'access_key=AKIA', 'region=eu')
//! SELECT * FROM read_csv('/x.csv', 'delimiter=;')
//! ```
//!
//! The positional `'key=value'` form is what `parse_file_tvf_args` (Task 2)
//! already accepts.  Only the file-reader TVFs listed in [`FILE_TVFS`] are
//! rewritten; every other FROM clause is left untouched.
//!
//! The rewriter is idempotent: running it twice on already-rewritten SQL is a
//! no-op (the path literal is `Unnamed`, not `Named` or a `BinaryExpr(Eq)`).
//!
//! # Depth guard
//!
//! This module does NOT call `check_expression_depth` itself.  When this
//! rewriter is wired into the coordinator SQL pipeline (Task 4), the caller
//! should call `check_expression_depth` on the parsed statements before
//! running the VisitMut walk, exactly as `rewrite_trino_compat` does.  That
//! guard is already `pub` in `crate::trino_compat`.

use std::ops::ControlFlow;

use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, Ident, TableFactor, TableFunctionArgs,
    Value, VisitMut, VisitorMut,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// The set of file-reader TVF names (lowercase) whose named arguments are
/// rewritten.  Matching is case-insensitive on the final identifier segment of
/// the function name.
///
/// `read_delta` is temporarily unwired in `sqe-catalog` for the DF54 bump but
/// is included here because the rewrite is purely syntactic and harmless, and
/// users may already have SQL that passes named args to it.
const FILE_TVFS: &[&str] = &["read_parquet", "read_csv", "read_json", "read_delta"];

/// Rewrite named TVF arguments in `sql` to positional `'key=value'` string
/// literals. Returns the rewritten SQL as a new string. If the input does not
/// parse, returns the original string unchanged so the downstream planner
/// surfaces its own error message.
pub fn rewrite_named_tvf_args(sql: &str) -> String {
    let dialect = GenericDialect {};
    let mut statements = match Parser::parse_sql(&dialect, sql) {
        Ok(s) => s,
        Err(_) => return sql.to_string(),
    };

    let mut visitor = TvfNamedArgsVisitor::default();
    let _ = VisitMut::visit(&mut statements, &mut visitor);

    if visitor.rewrites == 0 {
        // No rewrite fired. Avoid the round-trip through Display: some SQL
        // constructs do not round-trip exactly, so leaving the original string
        // alone preserves user formatting and avoids unintended changes.
        return sql.to_string();
    }

    statements
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>()
        .join("; ")
}

/// VisitorMut that rewrites named/binary-eq TVF args for the file-reader TVFs.
#[derive(Default)]
struct TvfNamedArgsVisitor {
    rewrites: usize,
}

impl VisitorMut for TvfNamedArgsVisitor {
    type Break = ();

    fn post_visit_table_factor(
        &mut self,
        factor: &mut TableFactor,
    ) -> ControlFlow<Self::Break> {
        if rewrite_named_args_in_factor(factor) {
            self.rewrites += 1;
        }
        ControlFlow::Continue(())
    }
}

/// Return true if `factor` is a file-reader TVF call with at least one named
/// or binary-eq argument that was rewritten.
fn rewrite_named_args_in_factor(factor: &mut TableFactor) -> bool {
    let TableFactor::Table { name, args, .. } = factor else {
        return false;
    };

    let Some(TableFunctionArgs { args: fn_args, .. }) = args else {
        return false;
    };

    // Check whether the function name's last segment matches a file TVF.
    let is_file_tvf = name
        .0
        .last()
        .and_then(|p| p.as_ident())
        .map(|ident| {
            let lower = ident.value.to_ascii_lowercase();
            FILE_TVFS.contains(&lower.as_str())
        })
        .unwrap_or(false);

    if !is_file_tvf {
        return false;
    }

    let mut rewrote_any = false;
    for arg in fn_args.iter_mut() {
        if rewrite_one_arg(arg) {
            rewrote_any = true;
        }
    }
    rewrote_any
}

/// Attempt to rewrite a single `FunctionArg` in place.  Returns true if the
/// arg was replaced with a positional `'key=value'` string literal.
fn rewrite_one_arg(arg: &mut FunctionArg) -> bool {
    match arg {
        // Arrow form: `key => value`
        // sqlparser 0.62 with GenericDialect parses this as
        // `FunctionArg::Named { name: Ident, arg: FunctionArgExpr::Expr(Expr::Value(..)), .. }`.
        FunctionArg::Named {
            name,
            arg: FunctionArgExpr::Expr(value_expr),
            ..
        } => {
            let key = name.value.clone();
            if let Some(combined) = extract_and_combine(&key, value_expr) {
                *arg = positional_string_arg(combined);
                return true;
            }
        }

        // Equals form: `key = value`
        // sqlparser 0.62 with GenericDialect does NOT parse this as a named
        // arg; instead it becomes
        // `FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::BinaryOp { left: Identifier, op: Eq, right: Value }))`.
        // Note: in sqlparser 0.62 `BinaryOp` is an inline struct variant, not
        // a tuple wrapping a separate struct type.
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::BinaryOp { left, op, right }))
            if *op == BinaryOperator::Eq =>
        {
            let key = match left.as_ref() {
                Expr::Identifier(Ident { value, .. }) => value.clone(),
                // CompoundIdentifier or any other form: leave this arg alone.
                _ => return false,
            };
            if let Some(combined) = extract_and_combine(&key, right) {
                *arg = positional_string_arg(combined);
                return true;
            }
        }

        _ => {}
    }
    false
}

/// Extract the raw string content from a literal `Expr::Value(...)` and
/// combine it with `key` into `"key=value"`.  Returns `None` if the expression
/// is not a supported literal type (in which case the arg is left unchanged).
fn extract_and_combine(key: &str, value_expr: &Expr) -> Option<String> {
    let Expr::Value(vws) = value_expr else {
        return None;
    };
    let rendered = match &vws.value {
        Value::SingleQuotedString(s) => s.clone(),
        Value::DoubleQuotedString(s) => s.clone(),
        Value::Number(n, _) => n.clone(),
        Value::Boolean(b) => b.to_string(),
        // Leave any other value type (NULL, hex literals, etc.) unchanged.
        _ => return None,
    };
    Some(format!("{key}={rendered}"))
}

/// Build a positional `FunctionArg::Unnamed(Expr::Value(SingleQuotedString(s)))`.
/// The `.into()` call builds the `ValueWithSpan` wrapper that sqlparser 0.62
/// requires (mirrors the construction used in `trino_compat.rs`).
fn positional_string_arg(s: String) -> FunctionArg {
    FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
        Value::SingleQuotedString(s).into(),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrites_named_arrow_args() {
        let out = rewrite_named_tvf_args(
            "SELECT * FROM read_parquet('s3://b/x.parquet', access_key => 'AKIA', region => 'eu')",
        );
        assert!(out.contains("'access_key=AKIA'"), "got: {out}");
        assert!(out.contains("'region=eu'"), "got: {out}");
        assert!(!out.contains("=>"), "no named args remain: {out}");
        assert!(out.contains("'s3://b/x.parquet'"), "path preserved: {out}");
    }

    #[test]
    fn rewrites_eq_binary_args() {
        let out = rewrite_named_tvf_args("SELECT * FROM read_csv('/x.csv', delimiter = ';')");
        assert!(out.contains("'delimiter=;'"), "got: {out}");
    }

    #[test]
    fn covers_ctas_inner_select() {
        let out = rewrite_named_tvf_args(
            "CREATE TABLE t AS SELECT * FROM read_parquet('s3://b/p.parquet', access_key => 'k')",
        );
        assert!(out.contains("'access_key=k'"), "CTAS inner TVF rewritten: {out}");
    }

    #[test]
    fn leaves_non_tvf_calls_untouched() {
        // A non-file-TVF function call must not be rewritten.
        let out = rewrite_named_tvf_args("SELECT * FROM generate_series(1, 10)");
        assert!(
            out.to_lowercase().contains("generate_series(1, 10)"),
            "got: {out}"
        );
    }

    #[test]
    fn unparseable_returned_unchanged() {
        assert_eq!(rewrite_named_tvf_args("NOT SQL ;;;"), "NOT SQL ;;;");
    }

    // Additional coverage

    #[test]
    fn read_json_and_read_delta_rewritten() {
        let cases = [
            "SELECT * FROM read_json('s3://b/f.json', access_key => 'K')",
            "SELECT * FROM read_delta('s3://b/d/', access_key => 'K')",
        ];
        for sql in cases {
            let out = rewrite_named_tvf_args(sql);
            assert!(
                out.contains("'access_key=K'"),
                "expected rewrite for: {sql}\ngot: {out}"
            );
        }
    }

    #[test]
    fn multiple_named_args_all_rewritten() {
        let out = rewrite_named_tvf_args(
            "SELECT * FROM read_parquet('s3://b/x.parquet', access_key => 'AK', secret_key => 'SK', region => 'us-east-1')",
        );
        assert!(out.contains("'access_key=AK'"), "got: {out}");
        assert!(out.contains("'secret_key=SK'"), "got: {out}");
        assert!(out.contains("'region=us-east-1'"), "got: {out}");
        assert!(!out.contains("=>"), "no named args remain: {out}");
    }

    #[test]
    fn path_arg_unnamed_is_preserved() {
        // The first positional path arg must not be mangled.
        let out = rewrite_named_tvf_args(
            "SELECT * FROM read_parquet('s3://bucket/file.parquet', region => 'eu-west-1')",
        );
        assert!(out.contains("'s3://bucket/file.parquet'"), "got: {out}");
        assert!(out.contains("'region=eu-west-1'"), "got: {out}");
    }

    #[test]
    fn numeric_value_rendered_without_quotes() {
        // A numeric arg value should be rendered as a bare number inside the
        // combined string literal, not double-quoted.
        let out =
            rewrite_named_tvf_args("SELECT * FROM read_csv('/x.csv', skip_rows => 2)");
        assert!(out.contains("'skip_rows=2'"), "got: {out}");
    }

    #[test]
    fn case_insensitive_tvf_name() {
        // TVF names are compared case-insensitively.
        let out = rewrite_named_tvf_args(
            "SELECT * FROM READ_PARQUET('s3://b/x.parquet', region => 'eu')",
        );
        assert!(out.contains("'region=eu'"), "case-insensitive tvf match: {out}");
    }

    #[test]
    fn no_rewrite_for_plain_select_no_tvf() {
        let sql = "SELECT a, b FROM t WHERE c > 5";
        assert_eq!(rewrite_named_tvf_args(sql), sql);
    }
}
