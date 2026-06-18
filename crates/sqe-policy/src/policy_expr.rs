//! Parse a SQL boolean / scalar expression string into a DataFusion `Expr`,
//! schema-free. Used for Ranger `filterExpr` (row filters) and CUSTOM
//! `valueExpr` (column masks). Unqualified identifiers become unresolved
//! `Expr::Column`; they resolve later when the rewriter injects the expr into
//! a `Filter`/projection above the matching `TableScan`.

use std::collections::HashSet;

use arrow::datatypes::{DataType, Field, Fields};
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::logical_expr::Expr;
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::DFParser;
use datafusion::sql::sqlparser::ast::Expr as SqlExpr;

/// Parse `sql` (a single SQL expression, NOT a full statement) into an `Expr`.
/// Returns `Err` if the string is not a parseable expression. Callers MUST
/// fail closed on `Err` (reject the policy) rather than ignore the filter.
///
/// Implementation uses a two-pass approach:
///   1. Parse with `DFParser::parse_sql_into_expr` to obtain the sqlparser AST.
///      This catches syntax errors and trailing garbage (fail-closed).
///   2. Walk the AST to collect all column-reference identifiers.
///   3. Build an UNQUALIFIED stub `DFSchema` containing those column names (all
///      as `Utf8`) via `DFSchema::from_unqualified_fields`. Unqualified means
///      each column gets `relation: None`, so when the expr is later spliced
///      into a `Filter` above a real `TableScan`, DataFusion matches by name
///      alone and does not compare against a fake table qualifier.
///   4. Call `SessionContext::parse_sql_expr` with that schema so DataFusion's
///      `validate_schema_satisfies_exprs` pass succeeds without real schema info.
///
/// Column type coercion is not performed here; the resulting `Expr::Column`
/// refs carry no type information and are resolved when the rewriter splices
/// the expression into a `Filter`/`Projection` above the matching `TableScan`.
pub fn parse_sql_predicate(sql: &str) -> sqe_core::Result<Expr> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return Err(sqe_core::error::SqeError::Execution(
            "empty policy expression".to_string(),
        ));
    }

    // Pass 1: syntactic parse via DFParser (catches garbage + trailing tokens).
    let parsed = DFParser::parse_sql_into_expr(trimmed).map_err(|e| {
        sqe_core::error::SqeError::Execution(format!(
            "failed to parse Ranger policy expression '{trimmed}': {e}"
        ))
    })?;

    // Pass 2: collect all unqualified identifiers from the sqlparser AST.
    let mut col_names: HashSet<String> = HashSet::new();
    collect_identifiers(&parsed.expr, &mut col_names);

    // Build an UNQUALIFIED stub DFSchema so validate_schema_satisfies_exprs
    // passes without a real table schema. Fields are Utf8 (nullable); actual
    // types are resolved when the expr is injected into a plan above the real
    // TableScan. Using from_unqualified_fields (not try_from_qualified_schema)
    // is critical: qualified fields stamp a fake table name onto every
    // Expr::Column, which causes FieldNotFound when the expr is later spliced
    // into a Filter above a real scan (e.g. "employees.tier" != "__policy_stub__.tier").
    let fields: Fields = col_names
        .iter()
        .map(|name| Field::new(name.as_str(), DataType::Utf8, true))
        .collect();
    let df_schema = DFSchemaRef::new(
        DFSchema::from_unqualified_fields(fields, Default::default()).map_err(|e| {
            sqe_core::error::SqeError::Execution(format!(
                "failed to build policy stub schema for '{trimmed}': {e}"
            ))
        })?,
    );

    // Pass 3: build the DataFusion Expr using the stub schema.
    let ctx = SessionContext::new();
    ctx.parse_sql_expr(trimmed, &df_schema).map_err(|e| {
        sqe_core::error::SqeError::Execution(format!(
            "failed to plan Ranger policy expression '{trimmed}': {e}"
        ))
    })
}

/// Recursively collect all bare identifier names referenced in `expr`.
/// Only `Identifier` nodes are harvested; `CompoundIdentifier` nodes (e.g.
/// `table.column`) are not expected in schema-free policy expressions and
/// are intentionally left out — the planner handles them as qualified refs.
fn collect_identifiers(expr: &SqlExpr, names: &mut HashSet<String>) {
    match expr {
        SqlExpr::Identifier(ident) => {
            // Normalise to lowercase, matching DataFusion's default ident
            // normalization (`enable_ident_normalization = true`).
            names.insert(ident.value.to_lowercase());
        }
        SqlExpr::BinaryOp { left, right, .. } => {
            collect_identifiers(left, names);
            collect_identifiers(right, names);
        }
        SqlExpr::UnaryOp { expr, .. } => {
            collect_identifiers(expr, names);
        }
        SqlExpr::IsNull(e) | SqlExpr::IsNotNull(e) => {
            collect_identifiers(e, names);
        }
        SqlExpr::Between {
            expr, low, high, ..
        } => {
            collect_identifiers(expr, names);
            collect_identifiers(low, names);
            collect_identifiers(high, names);
        }
        SqlExpr::InList { expr, list, .. } => {
            collect_identifiers(expr, names);
            for item in list {
                collect_identifiers(item, names);
            }
        }
        SqlExpr::Function(f) => {
            use datafusion::sql::sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};
            if let FunctionArguments::List(arg_list) = &f.args {
                for item in &arg_list.args {
                    let arg_expr = match item {
                        FunctionArg::Named { arg, .. }
                        | FunctionArg::Unnamed(arg)
                        | FunctionArg::ExprNamed { arg, .. } => arg,
                    };
                    match arg_expr {
                        FunctionArgExpr::Expr(e) => collect_identifiers(e, names),
                        FunctionArgExpr::Wildcard
                        | FunctionArgExpr::QualifiedWildcard(_)
                        | FunctionArgExpr::WildcardWithOptions(_) => {}
                    }
                }
            }
        }
        SqlExpr::Nested(e) => {
            collect_identifiers(e, names);
        }
        SqlExpr::Like { expr, pattern, .. }
        | SqlExpr::ILike { expr, pattern, .. }
        | SqlExpr::SimilarTo { expr, pattern, .. } => {
            collect_identifiers(expr, names);
            collect_identifiers(pattern, names);
        }
        SqlExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(e) = operand {
                collect_identifiers(e, names);
            }
            for c in conditions {
                collect_identifiers(&c.condition, names);
                collect_identifiers(&c.result, names);
            }
            if let Some(e) = else_result {
                collect_identifiers(e, names);
            }
        }
        // Everything else (literals, typed strings, etc.) has no identifiers.
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_single_comparison() {
        let e = parse_sql_predicate("clearance >= 3").unwrap();
        assert!(matches!(e, Expr::BinaryExpr(_)));
    }

    #[test]
    fn parses_compound_and() {
        // The case the toy parser silently corrupts.
        let e = parse_sql_predicate("region = 'EU' AND tier < 3").unwrap();
        assert!(matches!(e, Expr::BinaryExpr(_)));
        let sql = datafusion::sql::unparser::expr_to_sql(&e).unwrap().to_string();
        assert!(sql.contains("region"));
        assert!(sql.contains("tier"));
        assert!(sql.to_uppercase().contains("AND"));
    }

    #[test]
    fn parses_in_list() {
        let e = parse_sql_predicate("dept IN ('hr', 'eng')").unwrap();
        let sql = datafusion::sql::unparser::expr_to_sql(&e).unwrap().to_string();
        assert!(sql.to_uppercase().contains("IN"));
    }

    #[test]
    fn parses_custom_mask_valueexpr() {
        // CUSTOM mask bodies are scalar exprs, often a function call.
        let e = parse_sql_predicate("concat('***', email)").unwrap();
        assert!(matches!(e, Expr::ScalarFunction(_)));
    }

    #[test]
    fn rejects_garbage() {
        assert!(parse_sql_predicate("this is not sql !!!").is_err());
    }

    #[test]
    fn rejects_empty() {
        assert!(parse_sql_predicate("").is_err());
        assert!(parse_sql_predicate("   ").is_err());
    }

    #[test]
    fn rejects_trailing_garbage() {
        // Fail-closed: a valid prefix followed by junk must not silently parse
        // to just the prefix. DFParser::parse_into_expr enforces EOF after the
        // expression, so this must return Err.
        assert!(parse_sql_predicate("region = 'EU' bogus").is_err());
    }
}
