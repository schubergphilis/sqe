//! DuckDB's bare `FROM t`, short for `SELECT * FROM t`.
//!
//! sqlparser parses `FROM t` into a `Select` with an empty projection
//! (`SelectFlavor::FromFirstNoSelect`). DataFusion plans that as zero output
//! columns, so the query ran and every client printed "0 rows". The front
//! door turns it into a standard `SELECT *` at any depth: CTEs, subqueries,
//! set-operation branches and `INSERT ... FROM t`.

use std::ops::ControlFlow;

use sqlparser::ast::{
    Query, SelectFlavor, SelectItem, SetExpr, Statement, VisitMut, VisitorMut,
    WildcardAdditionalOptions,
};

/// Rewrite every bare `FROM t` in `statements` to `SELECT * FROM t`.
/// Returns true when anything changed.
pub fn expand_from_first_select(statements: &mut [Statement]) -> bool {
    let mut visitor = FromFirstExpander { changed: false };
    for stmt in statements.iter_mut() {
        let _ = stmt.visit(&mut visitor);
    }
    visitor.changed
}

struct FromFirstExpander {
    changed: bool,
}

impl VisitorMut for FromFirstExpander {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        self.changed |= expand_body(query.body.as_mut());
        ControlFlow::Continue(())
    }
}

/// Expand the selects of one query body. A nested `SetExpr::Query` is its own
/// `Query`, which the visitor reaches separately.
fn expand_body(body: &mut SetExpr) -> bool {
    match body {
        SetExpr::Select(select) if select.flavor == SelectFlavor::FromFirstNoSelect => {
            select.flavor = SelectFlavor::Standard;
            select.projection = vec![SelectItem::Wildcard(WildcardAdditionalOptions::default())];
            true
        }
        SetExpr::SetOperation { left, right, .. } => {
            let l = expand_body(left.as_mut());
            let r = expand_body(right.as_mut());
            l || r
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    fn expand(sql: &str) -> (bool, String) {
        let mut stmts = Parser::parse_sql(&GenericDialect {}, sql).expect(sql);
        let changed = expand_from_first_select(&mut stmts);
        (changed, stmts[0].to_string())
    }

    #[test]
    fn bare_from_selects_every_column() {
        assert_eq!(
            expand("FROM marts.revenue_by_month"),
            (true, "SELECT * FROM marts.revenue_by_month".to_string())
        );
    }

    #[test]
    fn keeps_order_by_and_limit() {
        assert_eq!(
            expand("FROM t ORDER BY a LIMIT 5"),
            (true, "SELECT * FROM t ORDER BY a LIMIT 5".to_string())
        );
    }

    #[test]
    fn reaches_ctes_subqueries_unions_and_inserts() {
        let (changed, sql) =
            expand("WITH c AS (FROM a) SELECT x FROM (FROM b) AS s UNION ALL FROM c");
        assert!(changed);
        assert_eq!(
            sql,
            "WITH c AS (SELECT * FROM a) SELECT x FROM (SELECT * FROM b) AS s UNION ALL SELECT * FROM c"
        );
        let (changed, sql) = expand("INSERT INTO t2 FROM t1");
        assert!(changed);
        assert_eq!(sql, "INSERT INTO t2 SELECT * FROM t1");
    }

    #[test]
    fn leaves_from_first_with_select_and_plain_select_alone() {
        assert!(!expand("FROM t SELECT a").0);
        assert!(!expand("SELECT a FROM t").0);
    }
}
