//! Lateral column aliases: a later select-list item naming an earlier
//! item's alias (`SELECT a + 1 AS b, b * 2 AS c FROM t`, Databricks,
//! Snowflake, Spark 3.4+).
//!
//! DataFusion resolves a select-list name against the `FROM` relation only,
//! so `b` above is "No field named b". The planner retries with
//! [`inline_lateral_alias`] for exactly the name that error reports: the
//! alias's expression replaces the bare identifier, in parentheses, so
//! `b * 2` becomes `(a + 1) * 2`, never `a + 1 * 2`.
//!
//! Driving the rewrite from the planner's error gives Databricks' precedence
//! rule for free: a name the `FROM` relation has is the column, never the
//! alias (`price * 2 AS price, price + 1 AS q` reads the column), because
//! that name never fails to resolve. The error does not say which `SELECT`
//! failed, so the name is inlined in every `SELECT` that defines it as an
//! earlier alias. A `SELECT` where the name is both a `FROM` column and an
//! earlier alias, in a statement where another `SELECT` lacks it, would read
//! the alias there; that shape is ambiguous in Databricks too.
//!
//! Only bare identifiers in the select list are replaced. Qualified names
//! (`t.b`), subqueries (their own scope) and lambda parameters of the same
//! name are left alone. `WHERE` cannot see a select-list alias in Databricks
//! either; `GROUP BY`, `HAVING` and `ORDER BY` aliases DataFusion already
//! resolves.
use std::ops::ControlFlow;

use sqlparser::ast::{Expr, Ident, Query, Select, SelectItem, SetExpr, VisitMut, VisitorMut};
use sqlparser::parser::Parser;

use crate::front_door::ExecutionDialect;

/// `sql` with every bare reference to `name` in a select list replaced by
/// the expression an earlier item of that select list aliases as `name`.
/// `None` when `sql` is not one statement or no select list defines `name`
/// before using it, so the caller keeps its original error.
///
/// `name` is the planner's normalized name: unquoted identifiers compare
/// ASCII-case-insensitively, quoted ones exactly.
pub fn inline_lateral_alias(sql: &str, name: &str) -> Option<String> {
    // The planner's dialect (DuckDB plus `|>`, #851), so a pipe query
    // parses here too.
    let mut statements = Parser::parse_sql(&ExecutionDialect, sql).ok()?;
    let [statement] = statements.as_mut_slice() else {
        return None;
    };
    let mut selects = SelectInliner {
        name,
        changed: false,
    };
    let _ = statement.visit(&mut selects);
    selects.changed.then(|| statement.to_string())
}

/// Planner-normalized form of `ident`: DataFusion lowercases unquoted
/// identifiers (ASCII only, like `identifier_case`).
fn names(ident: &Ident, name: &str) -> bool {
    match ident.quote_style {
        Some(_) => ident.value == name,
        None => ident.value.eq_ignore_ascii_case(name),
    }
}

/// Visits every query (top level, CTEs, subqueries, set-operation arms) and
/// rewrites the select lists of its `SELECT`s.
struct SelectInliner<'a> {
    name: &'a str,
    changed: bool,
}

impl VisitorMut for SelectInliner<'_> {
    type Break = ();

    fn post_visit_query(&mut self, query: &mut Query) -> ControlFlow<()> {
        self.changed |= inline_in_set_expr(&mut query.body, self.name);
        ControlFlow::Continue(())
    }
}

/// A nested `SetExpr::Query` is its own query, visited on its own.
fn inline_in_set_expr(body: &mut SetExpr, name: &str) -> bool {
    match body {
        SetExpr::Select(select) => inline_in_select(select, name),
        SetExpr::SetOperation { left, right, .. } => {
            let left = inline_in_set_expr(left, name);
            inline_in_set_expr(right, name) | left
        }
        _ => false,
    }
}

fn inline_in_select(select: &mut Select, name: &str) -> bool {
    let mut definition: Option<Expr> = None;
    let mut changed = false;
    for item in &mut select.projection {
        let (expr, alias) = match item {
            SelectItem::UnnamedExpr(expr) => (expr, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(&*alias)),
            _ => continue,
        };
        if let Some(definition) = &definition {
            let mut references = References {
                name,
                definition,
                shadowed: 0,
                changed: false,
            };
            let _ = expr.visit(&mut references);
            changed |= references.changed;
        }
        if definition.is_none() && alias.is_some_and(|alias| names(alias, name)) {
            definition = Some(Expr::Nested(Box::new(expr.clone())));
        }
    }
    changed
}

/// Replaces bare `name` identifiers in one select-list expression, outside
/// subqueries and lambdas that bind `name`.
struct References<'a> {
    name: &'a str,
    definition: &'a Expr,
    shadowed: usize,
    changed: bool,
}

impl References<'_> {
    fn binds_name(&self, expr: &Expr) -> bool {
        match expr {
            Expr::Lambda(lambda) => lambda
                .params
                .iter()
                .any(|param| names(&param.name, self.name)),
            _ => false,
        }
    }
}

impl VisitorMut for References<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, _query: &mut Query) -> ControlFlow<()> {
        self.shadowed += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &mut Query) -> ControlFlow<()> {
        self.shadowed -= 1;
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        if self.binds_name(expr) {
            self.shadowed += 1;
        }
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        if self.binds_name(expr) {
            self.shadowed -= 1;
        } else if self.shadowed == 0 {
            if let Expr::Identifier(ident) = expr {
                if names(ident, self.name) {
                    *expr = self.definition.clone();
                    self.changed = true;
                }
            }
        }
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use super::inline_lateral_alias;

    fn inline(sql: &str, name: &str) -> Option<String> {
        inline_lateral_alias(sql, name)
    }

    #[test]
    fn later_item_reads_the_alias_in_parentheses() {
        let sql = "SELECT a + 1 AS b, b * 2 AS c FROM t";
        let out = inline(sql, "b").unwrap();
        assert_eq!(out, "SELECT a + 1 AS b, (a + 1) * 2 AS c FROM t");
    }

    #[test]
    fn chains_resolve_one_name_per_round() {
        let sql = "SELECT a + 1 AS b, b * 2 AS c, c + b AS d FROM t";
        let round1 = inline(sql, "b").unwrap();
        assert_eq!(
            round1,
            "SELECT a + 1 AS b, (a + 1) * 2 AS c, c + (a + 1) AS d FROM t"
        );
        let round2 = inline(&round1, "c").unwrap();
        let want = "SELECT a + 1 AS b, (a + 1) * 2 AS c, ((a + 1) * 2) + (a + 1) AS d FROM t";
        assert_eq!(round2, want);
    }

    #[test]
    fn only_earlier_aliases_and_bare_names_are_inlined() {
        // Defined after its use: not lateral.
        assert_eq!(inline("SELECT b * 2 AS c, a + 1 AS b FROM t", "b"), None);
        // Self-reference is the defining item, never rewritten.
        assert_eq!(inline("SELECT b + 1 AS b FROM t", "b"), None);
        // Qualified: always the relation's column.
        assert_eq!(inline("SELECT a + 1 AS b, t.b * 2 AS c FROM t", "b"), None);
        // Unknown name: the caller keeps its error.
        assert_eq!(inline("SELECT a + 1 AS b, z AS c FROM t", "z"), None);
    }

    #[test]
    fn subqueries_and_lambdas_keep_their_own_scope() {
        let sql = "SELECT a AS b, (SELECT max(b) FROM u) AS c, b AS d FROM t";
        let out = inline(sql, "b").unwrap();
        assert_eq!(
            out,
            "SELECT a AS b, (SELECT max(b) FROM u) AS c, (a) AS d FROM t"
        );

        let sql = "SELECT a AS b, list_transform(xs, b -> b + 1) AS c FROM t";
        assert_eq!(inline(sql, "b"), None);
    }

    #[test]
    fn every_select_that_defines_the_alias_is_rewritten() {
        let sql = "WITH w AS (SELECT a AS b, b + 1 AS c FROM t) \
                   SELECT x AS b, b * 2 AS y FROM w UNION ALL SELECT 1, 2";
        let out = inline(sql, "b").unwrap();
        assert!(out.contains("(a) + 1 AS c"), "{out}");
        assert!(out.contains("(x) * 2 AS y"), "{out}");
    }

    #[test]
    fn aggregates_and_case_folding() {
        let sql = "SELECT g, SUM(x) AS S, s * 2 AS u FROM t GROUP BY g";
        let out = inline(sql, "s").unwrap();
        assert_eq!(
            out,
            "SELECT g, SUM(x) AS S, (SUM(x)) * 2 AS u FROM t GROUP BY g"
        );
        // A quoted alias only matches its exact spelling.
        assert_eq!(inline("SELECT 1 AS \"S\", s AS u FROM t", "s"), None);
    }

    #[test]
    fn several_statements_are_left_alone() {
        assert_eq!(inline("SELECT 1 AS b, b; SELECT 2", "b"), None);
        assert_eq!(inline("not sql at all", "b"), None);
    }
}
