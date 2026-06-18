//! Walk a parsed SQL statement and collect the catalog component of any
//! 3-part identifier referenced by table-like nodes.
//!
//! Background: a 3-part identifier `cat.ns.t` lets a user route a query
//! at a specific catalog without changing the session-default. DataFusion
//! resolves the identifier through its registered `CatalogProviderList`,
//! and when the catalog component does not match a registered name it
//! silently falls back to the session-default catalog. Users then see a
//! confusing "namespace does not exist" error against the wrong
//! warehouse.
//!
//! The coordinator runs this walk pre-planning to detect unknown
//! qualifiers and turn the silent misroute into a clear planning-time
//! error. The walk uses sqlparser's `Visitor` trait so we cover every
//! `ObjectName` the parser exposes (table refs, view refs, target tables
//! in INSERT/MERGE/UPDATE/DELETE, etc.) without enumerating every
//! `Statement` variant by hand.

use std::collections::BTreeSet;
use std::ops::ControlFlow;

use sqlparser::ast::{ObjectName, Statement, Visit, Visitor};

/// Collect every distinct catalog qualifier from 3-part identifiers in
/// `stmt`. Returns an empty `Vec` when the statement only uses 1- or
/// 2-part names.
///
/// Idents are normalized by stripping any surrounding quote chars so
/// `"tf_main_warehouse".ns.t` and `tf_main_warehouse.ns.t` both report
/// the qualifier as `tf_main_warehouse`.
///
/// Identifiers with more than 3 parts (e.g. `a.b.c.d`) are ignored
/// here; the classifier's resource-reference parser already rejects
/// them at parse time.
pub fn extract_catalog_qualifiers(stmt: &Statement) -> Vec<String> {
    let mut visitor = CatalogQualifierCollector {
        catalogs: BTreeSet::new(),
    };
    let _ = stmt.visit(&mut visitor);
    visitor.catalogs.into_iter().collect()
}

/// Like [`extract_catalog_qualifiers`] but takes raw SQL text — used by the
/// view planner, which only holds the view's stored SQL string (not a parsed
/// statement). Unions qualifiers across all parsed statements; returns an
/// empty `Vec` when the SQL fails to parse (the caller's own planner will
/// surface the real parse error).
pub fn extract_catalog_qualifiers_from_sql(sql: &str) -> Vec<String> {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    let Ok(statements) = Parser::parse_sql(&GenericDialect {}, sql) else {
        return Vec::new();
    };
    let mut all = BTreeSet::new();
    for stmt in &statements {
        all.extend(extract_catalog_qualifiers(stmt));
    }
    all.into_iter().collect()
}

struct CatalogQualifierCollector {
    catalogs: BTreeSet<String>,
}

impl Visitor for CatalogQualifierCollector {
    type Break = ();

    fn pre_visit_relation(&mut self, name: &ObjectName) -> ControlFlow<Self::Break> {
        if name.0.len() == 3 {
            // Only the leading component is the catalog. Strip any
            // backtick / double-quote wrapping so the comparison
            // against `ctx.catalog_names()` works for both
            // `"cat".ns.t` and bare `cat.ns.t` shapes.
            if let Some(ident) = name.0[0].as_ident() {
                self.catalogs.insert(ident.value.clone());
            }
        }
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    fn parse(sql: &str) -> Statement {
        let dialect = GenericDialect {};
        Parser::parse_sql(&dialect, sql)
            .expect("parse")
            .into_iter()
            .next()
            .expect("at least one statement")
    }

    #[test]
    fn unqualified_table_returns_empty() {
        let stmt = parse("SELECT * FROM foo");
        assert!(extract_catalog_qualifiers(&stmt).is_empty());
    }

    #[test]
    fn two_part_identifier_returns_empty() {
        let stmt = parse("SELECT * FROM ns.t");
        assert!(extract_catalog_qualifiers(&stmt).is_empty());
    }

    #[test]
    fn three_part_identifier_extracts_catalog() {
        let stmt = parse("SELECT * FROM cat.ns.t");
        assert_eq!(extract_catalog_qualifiers(&stmt), vec!["cat".to_string()]);
    }

    #[test]
    fn quoted_three_part_identifier_extracts_catalog() {
        // sqlparser stores the unquoted value in `Ident::value`,
        // so the collector reports the bare name regardless of
        // how the user spelled it.
        let stmt = parse("SELECT * FROM \"tf_main_warehouse\".ns.t");
        assert_eq!(
            extract_catalog_qualifiers(&stmt),
            vec!["tf_main_warehouse".to_string()]
        );
    }

    #[test]
    fn join_with_two_three_part_identifiers_extracts_both() {
        let stmt =
            parse("SELECT * FROM cat_a.ns.t1 JOIN cat_b.ns.t2 ON t1.id = t2.id");
        let mut got = extract_catalog_qualifiers(&stmt);
        got.sort();
        assert_eq!(got, vec!["cat_a".to_string(), "cat_b".to_string()]);
    }

    #[test]
    fn duplicate_qualifiers_are_deduplicated() {
        let stmt =
            parse("SELECT * FROM cat.ns.t1 JOIN cat.ns.t2 ON t1.id = t2.id");
        assert_eq!(extract_catalog_qualifiers(&stmt), vec!["cat".to_string()]);
    }

    #[test]
    fn insert_into_three_part_target_extracts_catalog() {
        let stmt = parse("INSERT INTO cat.ns.t SELECT 1");
        assert_eq!(extract_catalog_qualifiers(&stmt), vec!["cat".to_string()]);
    }

    #[test]
    fn ctas_three_part_target_extracts_catalog() {
        let stmt = parse("CREATE TABLE cat.ns.t AS SELECT 1");
        assert_eq!(extract_catalog_qualifiers(&stmt), vec!["cat".to_string()]);
    }

    #[test]
    fn delete_three_part_target_extracts_catalog() {
        let stmt = parse("DELETE FROM cat.ns.t WHERE id = 1");
        assert_eq!(extract_catalog_qualifiers(&stmt), vec!["cat".to_string()]);
    }

    #[test]
    fn update_three_part_target_extracts_catalog() {
        let stmt = parse("UPDATE cat.ns.t SET v = 1 WHERE id = 1");
        assert_eq!(extract_catalog_qualifiers(&stmt), vec!["cat".to_string()]);
    }

    #[test]
    fn cte_with_three_part_table_extracts_catalog() {
        let stmt = parse(
            "WITH x AS (SELECT * FROM cat.ns.t) SELECT * FROM x",
        );
        assert_eq!(extract_catalog_qualifiers(&stmt), vec!["cat".to_string()]);
    }

    #[test]
    fn from_sql_extracts_catalogs_from_raw_text() {
        // The view-planner path: raw stored view SQL, possibly referencing a
        // catalog other than the one the view lives in.
        let got = extract_catalog_qualifiers_from_sql(
            "SELECT src.id AS event_id FROM team_a_data.public.events src",
        );
        assert_eq!(got, vec!["team_a_data".to_string()]);
    }

    #[test]
    fn from_sql_unparseable_returns_empty() {
        assert!(extract_catalog_qualifiers_from_sql("THIS IS NOT SQL ???").is_empty());
    }
}
