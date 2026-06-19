//! Parse a SQL boolean / scalar expression string into a DataFusion `Expr`,
//! schema-free. Used for Ranger `filterExpr` (row filters) and CUSTOM
//! `valueExpr` (column masks). Unqualified identifiers become unresolved
//! `Expr::Column`; they resolve later when the rewriter injects the expr into
//! a `Filter`/projection above the matching `TableScan`.

use std::collections::HashSet;
use std::ops::ControlFlow;

use arrow::datatypes::{DataType, Field, Fields};
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::logical_expr::Expr;
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::DFParser;
use datafusion::sql::sqlparser::ast::{Expr as SqlExpr, Visit, Visitor};

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
///
/// Only bare, unqualified column references are supported in this MVP. Qualified
/// or compound identifiers (e.g. `t.col`) are NOT supported: the stub schema is
/// unqualified, so a compound ref fails `validate_schema_satisfies_exprs` and
/// this function returns `Err` (fail closed). Ranger `filterExpr`/`valueExpr`
/// bodies use bare column names, so this is the expected shape.
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
            "failed to parse policy expression '{trimmed}': {e}"
        ))
    })?;

    // Pass 2: collect all unqualified identifiers from the sqlparser AST.
    // A Visitor is total by construction: it walks every AST variant (Cast,
    // Substring, Extract, Trim, Case, ...) so a column nested inside any node
    // is captured. A hand-written match with a catch-all would silently miss
    // columns in un-enumerated variants and wrongly reject valid policies.
    let mut col_names: HashSet<String> = HashSet::new();
    let _ = parsed.expr.visit(&mut IdentCollector(&mut col_names));

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
    //
    // ISSUE 3 note: constructing a fresh SessionContext per call is acceptable
    // for the MVP. parse_sql_predicate runs at policy-resolve frequency (i.e.
    // on a cache miss in the policy store), not per row or per batch, so the
    // per-call context setup cost is negligible against the surrounding I/O.
    let ctx = SessionContext::new();
    ctx.parse_sql_expr(trimmed, &df_schema).map_err(|e| {
        sqe_core::error::SqeError::Execution(format!(
            "failed to plan policy expression '{trimmed}': {e}"
        ))
    })
}

/// sqlparser `Visitor` that harvests every bare `Identifier` in an expression
/// tree into a set of column names. Compound identifiers (`table.column`) are
/// intentionally NOT collected: this MVP supports only bare column refs, and a
/// compound ref against the unqualified stub schema fails closed.
struct IdentCollector<'a>(&'a mut HashSet<String>);

impl Visitor for IdentCollector<'_> {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &SqlExpr) -> ControlFlow<()> {
        if let SqlExpr::Identifier(ident) = expr {
            // Only lowercase UNQUOTED identifiers. DataFusion's default ident
            // normalization lowercases unquoted names but preserves the case of
            // quoted ones. Lowercasing a quoted "Tier" here would put "tier" in
            // the stub while parse_sql_expr later looks up "Tier" -> FieldNotFound.
            let key = if ident.quote_style.is_none() {
                ident.value.to_lowercase()
            } else {
                ident.value.clone()
            };
            self.0.insert(key);
        }
        ControlFlow::Continue(())
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

    #[test]
    fn parses_cast_in_filter() {
        // Cast is its own AST variant; the hand-written walker missed it.
        let e = parse_sql_predicate("CAST(tier AS INT) >= 3").unwrap();
        let sql = datafusion::sql::unparser::expr_to_sql(&e).unwrap().to_string();
        assert!(sql.to_lowercase().contains("tier"));
    }

    #[test]
    fn parses_substring_mask() {
        let e = parse_sql_predicate("substr(email, 1, 3)").unwrap();
        let sql = datafusion::sql::unparser::expr_to_sql(&e).unwrap().to_string();
        assert!(sql.to_lowercase().contains("email"));
    }

    #[test]
    fn parses_or() {
        let e = parse_sql_predicate("region = 'EU' OR dept = 'eng'").unwrap();
        let sql = datafusion::sql::unparser::expr_to_sql(&e).unwrap().to_string();
        assert!(sql.to_uppercase().contains("OR"));
    }

    #[test]
    fn parses_quoted_mixed_case_column() {
        // Quoted ident must NOT be lowercased into the stub.
        let e = parse_sql_predicate("\"Tier\" >= 3").unwrap();
        assert!(matches!(e, datafusion::logical_expr::Expr::BinaryExpr(_)));
    }
}
