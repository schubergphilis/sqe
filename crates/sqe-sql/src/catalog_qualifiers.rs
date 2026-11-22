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
    let _ = visit_named_relations(stmt, &mut visitor);
    visitor.catalogs.into_iter().collect()
}

/// `stmt.visit(visitor)`, plus the object a `SHOW CREATE` names: sqlparser
/// does not visit that name as a relation, so a `SHOW CREATE TABLE
/// cat.ns.t` on a catalog the session has not attached yet found no
/// catalog to discover and reported the table missing (#921).
fn visit_named_relations<V: Visitor>(stmt: &Statement, visitor: &mut V) -> ControlFlow<V::Break> {
    stmt.visit(visitor)?;
    if let Statement::ShowCreate { obj_name, .. } = stmt {
        visitor.pre_visit_relation(obj_name)?;
    }
    ControlFlow::Continue(())
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

/// Rename catalog `from` to `to` in every 3-part relation (`from.ns.t`) and
/// 4-part column reference (`from.ns.t.c`) of `node`. An unquoted `from`
/// matches case-insensitively, a quoted one exactly. `to` is quoted when it
/// is not a plain lowercase identifier. Returns true when a name changed.
///
/// A view body stored by an embedded session names the catalog by its
/// `ATTACH ... AS` name; every reader resolves the warehouse name, so the
/// body is stored with that instead, and shown with the attach name again.
pub fn rename_catalog<V: sqlparser::ast::VisitMut>(node: &mut V, from: &str, to: &str) -> bool {
    use sqlparser::ast::{visit_expressions_mut, visit_relations_mut, Expr, Ident};

    let matches = |ident: &Ident| match ident.quote_style {
        None => ident.value.eq_ignore_ascii_case(from),
        Some(_) => ident.value == from,
    };
    let renamed = || match crate::quote::quote_ident_if_needed(to, '"') == to {
        true => Ident::new(to),
        false => Ident::with_quote('"', to),
    };
    let mut changed = false;
    let _ = visit_relations_mut(node, |name: &mut ObjectName| {
        if let [first, _, _] = name.0.as_mut_slice() {
            if first.as_ident().is_some_and(matches) {
                *first = sqlparser::ast::ObjectNamePart::Identifier(renamed());
                changed = true;
            }
        }
        ControlFlow::<()>::Continue(())
    });
    let _ = visit_expressions_mut(node, |expr: &mut Expr| {
        if let Expr::CompoundIdentifier(parts) = expr {
            if parts.len() == 4 && matches(&parts[0]) {
                parts[0] = renamed();
                changed = true;
            }
        }
        ControlFlow::<()>::Continue(())
    });
    changed
}

/// Collect every distinct `(catalog, schema)` pair from schema-qualified
/// table references in `sql`: `ns.t` yields `(None, "ns")`, `cat.ns.t`
/// yields `(Some("cat"), "ns")`. Bare names contribute nothing. Returns an
/// empty `Vec` when the SQL fails to parse.
///
/// Used by the coordinator's pre-plan step to refresh the table-name
/// snapshots of exactly the namespaces a statement will plan against
/// (architecture review 806).
pub fn extract_schema_qualifiers_from_sql(sql: &str) -> Vec<(Option<String>, String)> {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    let Ok(statements) = Parser::parse_sql(&GenericDialect {}, sql) else {
        return Vec::new();
    };
    let mut visitor = SchemaQualifierCollector {
        schemas: BTreeSet::new(),
    };
    for stmt in &statements {
        let _ = visit_named_relations(stmt, &mut visitor);
    }
    visitor.schemas.into_iter().collect()
}

struct SchemaQualifierCollector {
    schemas: BTreeSet<(Option<String>, String)>,
}

impl Visitor for SchemaQualifierCollector {
    type Break = ();

    fn pre_visit_relation(&mut self, name: &ObjectName) -> ControlFlow<Self::Break> {
        let idents: Vec<&str> = name
            .0
            .iter()
            .filter_map(|p| p.as_ident().map(|i| i.value.as_str()))
            .collect();
        match idents.as_slice() {
            [schema, _table] => {
                self.schemas.insert((None, schema.to_string()));
            }
            [catalog, schema, _table] => {
                self.schemas
                    .insert((Some(catalog.to_string()), schema.to_string()));
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
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

/// True when `stmt` names a relation `system.<schema>.<t>` whose schema is
/// not one of SQE's virtual `system` schemas (#901): only then can the
/// statement need a Polaris catalog named `system`, so only then is it worth
/// asking Polaris for one. `system.runtime.*` and friends never probe.
pub fn names_polaris_system_schema(stmt: &Statement) -> bool {
    struct Finder(bool);
    impl Visitor for Finder {
        type Break = ();
        fn pre_visit_relation(&mut self, name: &ObjectName) -> ControlFlow<Self::Break> {
            if let [cat, schema, _] = name.0.as_slice() {
                let (Some(cat), Some(schema)) = (cat.as_ident(), schema.as_ident()) else {
                    return ControlFlow::Continue(());
                };
                // An unquoted identifier folds to lowercase before DataFusion
                // resolves it (#337); a quoted one is taken as written.
                let is_system = match cat.quote_style {
                    None => cat
                        .value
                        .eq_ignore_ascii_case(sqe_core::system_catalog::SYSTEM_CATALOG),
                    Some(_) => cat.value == sqe_core::system_catalog::SYSTEM_CATALOG,
                };
                if is_system && !sqe_core::system_catalog::is_reserved_system_schema(&schema.value)
                {
                    self.0 = true;
                    return ControlFlow::Break(());
                }
            }
            ControlFlow::Continue(())
        }
    }
    let mut finder = Finder(false);
    let _ = visit_named_relations(stmt, &mut finder);
    finder.0
}

/// The namespace an `information_schema` read pins, when the statement is a
/// single SELECT whose WHERE clause has a top-level conjunct of exactly
/// `table_schema = '<literal>'` (either operand order, optionally qualified
/// as `t.table_schema`). Every other shape, including a `table_schema`
/// comparison nested under OR, returns `None`, and the caller refreshes every
/// namespace: a wider refresh is never wrong, only slower.
///
/// Used by the coordinator's pre-plan step (issue #664) so `SHOW COLUMNS`,
/// `DESCRIBE` and dbt's `get_columns_in_relation` refresh one namespace's
/// table snapshot instead of the whole catalog's.
pub fn pinned_table_schema_from_sql(sql: &str) -> Option<String> {
    use sqlparser::ast::{BinaryOperator, Expr, SetExpr, Value};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    fn conjuncts<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
        match expr {
            Expr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => {
                conjuncts(left, out);
                conjuncts(right, out);
            }
            Expr::Nested(inner) => conjuncts(inner, out),
            other => out.push(other),
        }
    }

    fn is_table_schema(expr: &Expr) -> bool {
        match expr {
            Expr::Identifier(id) => id.value.eq_ignore_ascii_case("table_schema"),
            Expr::CompoundIdentifier(parts) => parts
                .last()
                .is_some_and(|id| id.value.eq_ignore_ascii_case("table_schema")),
            _ => false,
        }
    }

    fn string_literal(expr: &Expr) -> Option<String> {
        match expr {
            Expr::Value(v) => match &v.value {
                Value::SingleQuotedString(s) => Some(s.clone()),
                _ => None,
            },
            _ => None,
        }
    }

    let statements = Parser::parse_sql(&GenericDialect {}, sql).ok()?;
    let [Statement::Query(query)] = statements.as_slice() else {
        return None;
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    let selection = select.selection.as_ref()?;
    let mut parts = Vec::new();
    conjuncts(selection, &mut parts);
    parts.into_iter().find_map(|c| match c {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            if is_table_schema(left) {
                string_literal(right)
            } else if is_table_schema(right) {
                string_literal(left)
            } else {
                None
            }
        }
        _ => None,
    })
}

/// The namespaces an `information_schema` read pins: the intersection of
/// every top-level WHERE conjunct that admits only literal `table_schema`
/// values, `table_schema = 'a'` (either operand order),
/// `table_schema IN ('a', 'b')` or an OR of those (dbt's `get_catalog` sends
/// the IN list). `None` when no conjunct pins it, in which case the caller
/// refreshes every namespace.
///
/// Only the pre-plan snapshot refresh reads this. The walk itself narrows
/// from the filters DataFusion pushes into the scan, so a wider answer here
/// is only slower, never wrong.
pub fn pinned_table_schemas_from_sql(sql: &str) -> Option<Vec<String>> {
    let select = single_select(sql)?;
    let values = pinned_conjunct_values(select.selection.as_ref()?, &["table_schema"])?;
    Some(values.into_iter().collect())
}

/// The one catalog an `information_schema` read is limited to (OpenSpec
/// `information-schema-visible-catalogs`, D4), taking the first match:
///
/// 1. the `information_schema` qualifier, `cat.information_schema.<view>`,
///    when every `information_schema` relation of the statement names the
///    same catalog. A two-part `information_schema.<view>` counts as
///    qualified with `session_catalog` when one is set, because the front
///    door qualifies it with that catalog before planning (#2);
/// 2. for a statement whose `information_schema` relations are all
///    unqualified (and no session catalog is set), a top-level conjunct
///    `table_catalog = '<lit>'` or `catalog_name = '<lit>'` (dbt-sqe's
///    `get_catalog`, `list_schemas` and `check_schema_exists`);
/// 3. `None`: the read walks every registered catalog.
///
/// An unquoted qualifier folds to lowercase as DataFusion folds it (#337).
/// The coordinator refreshes only the pinned catalog's snapshots before
/// planning; the walk narrows to the same catalog from the plan's own
/// table reference and filters.
pub fn pinned_catalog(sql: &str, session_catalog: Option<&str>) -> Option<String> {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    struct Qualifiers {
        catalogs: BTreeSet<String>,
        bare: bool,
        session_catalog: Option<String>,
    }
    impl Visitor for Qualifiers {
        type Break = ();
        fn pre_visit_relation(&mut self, name: &ObjectName) -> ControlFlow<Self::Break> {
            let idents: Vec<_> = name.0.iter().filter_map(|p| p.as_ident()).collect();
            match idents.as_slice() {
                [schema, _] if is_information_schema(schema) => match &self.session_catalog {
                    Some(catalog) => {
                        self.catalogs.insert(catalog.clone());
                    }
                    None => self.bare = true,
                },
                [catalog, schema, _] if is_information_schema(schema) => {
                    let name = match catalog.quote_style {
                        None => catalog.value.to_ascii_lowercase(),
                        Some(_) => catalog.value.clone(),
                    };
                    self.catalogs.insert(name);
                }
                _ => {}
            }
            ControlFlow::Continue(())
        }
    }
    fn is_information_schema(ident: &sqlparser::ast::Ident) -> bool {
        ident.value.eq_ignore_ascii_case("information_schema")
    }

    let statements = Parser::parse_sql(&GenericDialect {}, sql).ok()?;
    let [stmt] = statements.as_slice() else {
        return None;
    };
    let mut found = Qualifiers {
        catalogs: BTreeSet::new(),
        bare: false,
        session_catalog: session_catalog.map(str::to_string),
    };
    let _ = visit_named_relations(stmt, &mut found);
    match (found.bare, found.catalogs.len()) {
        (false, 1) => found.catalogs.into_iter().next(),
        (true, 0) => {
            let select = single_select(sql)?;
            let values = pinned_conjunct_values(
                select.selection.as_ref()?,
                &["table_catalog", "catalog_name"],
            )?;
            match values.len() {
                1 => values.into_iter().next(),
                _ => None,
            }
        }
        _ => None,
    }
}

/// The body of `sql` when it is exactly one plain `SELECT`.
fn single_select(sql: &str) -> Option<Box<sqlparser::ast::Select>> {
    use sqlparser::ast::SetExpr;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    let statements = Parser::parse_sql(&GenericDialect {}, sql).ok()?;
    let [Statement::Query(query)] = statements.as_slice() else {
        return None;
    };
    match query.body.as_ref() {
        SetExpr::Select(select) => Some(select.clone()),
        _ => None,
    }
}

/// The literals the top-level conjuncts of `selection` allow for a column
/// named one of `columns` (optionally qualified, `c.table_schema`): the
/// intersection over every conjunct that pins one. `None` when no conjunct
/// does.
fn pinned_conjunct_values(
    selection: &sqlparser::ast::Expr,
    columns: &[&str],
) -> Option<BTreeSet<String>> {
    use sqlparser::ast::{BinaryOperator, Expr};

    fn conjuncts<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
        match expr {
            Expr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => {
                conjuncts(left, out);
                conjuncts(right, out);
            }
            Expr::Nested(inner) => conjuncts(inner, out),
            other => out.push(other),
        }
    }

    let mut parts = Vec::new();
    conjuncts(selection, &mut parts);
    parts
        .into_iter()
        .filter_map(|c| literal_values(c, columns))
        .reduce(|a, b| a.intersection(&b).cloned().collect())
}

/// The string literals `expr` admits for one of `columns`, or `None` when
/// it admits anything else: `col = 'v'`, `col IN ('a', 'b')` (not negated,
/// literals only), or an OR of those on the same column.
fn literal_values(expr: &sqlparser::ast::Expr, columns: &[&str]) -> Option<BTreeSet<String>> {
    use sqlparser::ast::{BinaryOperator, Expr, Value};

    fn column(expr: &Expr) -> Option<String> {
        let ident = match expr {
            Expr::Identifier(id) => id,
            Expr::CompoundIdentifier(parts) => parts.last()?,
            _ => return None,
        };
        Some(ident.value.to_ascii_lowercase())
    }
    fn literal(expr: &Expr) -> Option<String> {
        match expr {
            Expr::Value(v) => match &v.value {
                Value::SingleQuotedString(s) => Some(s.clone()),
                _ => None,
            },
            _ => None,
        }
    }
    fn pinned(expr: &Expr) -> Option<(String, BTreeSet<String>)> {
        match expr {
            Expr::Nested(inner) => pinned(inner),
            Expr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } => match (column(left), column(right)) {
                (Some(c), None) => Some((c, BTreeSet::from([literal(right)?]))),
                (None, Some(c)) => Some((c, BTreeSet::from([literal(left)?]))),
                _ => None,
            },
            Expr::InList {
                expr,
                list,
                negated: false,
            } => {
                let values = list.iter().map(literal).collect::<Option<BTreeSet<_>>>()?;
                Some((column(expr)?, values))
            }
            Expr::BinaryOp {
                left,
                op: BinaryOperator::Or,
                right,
            } => {
                let (lc, mut lv) = pinned(left)?;
                let (rc, rv) = pinned(right)?;
                if lc != rc {
                    return None;
                }
                lv.extend(rv);
                Some((lc, lv))
            }
            _ => None,
        }
    }

    let (column, values) = pinned(expr)?;
    columns.contains(&column.as_str()).then_some(values)
}

#[cfg(test)]
mod pinned_catalog_tests {
    use super::{pinned_catalog, pinned_table_schemas_from_sql};

    #[test]
    fn the_information_schema_qualifier_pins_the_catalog() {
        let pin = |sql| pinned_catalog(sql, None);
        assert_eq!(
            pin("SELECT * FROM \"Lake\".information_schema.tables"),
            Some("Lake".into())
        );
        assert_eq!(
            pin("SELECT * FROM Lake.information_schema.tables"),
            Some("lake".into())
        );
        // dbt-sqe's get_catalog: two relations, one catalog.
        assert_eq!(
            pin(
                "SELECT c.table_name FROM \"dw\".information_schema.columns c \
                 JOIN \"dw\".information_schema.tables t ON t.table_name = c.table_name \
                 WHERE c.table_catalog = 'dw' AND c.table_schema IN ('a', 'b')"
            ),
            Some("dw".into())
        );
        // The qualifier wins over a conflicting conjunct, as in Trino: the
        // read is answered by `a` and its filter keeps no row.
        assert_eq!(
            pin("SELECT * FROM a.information_schema.tables WHERE table_catalog = 'b'"),
            Some("a".into())
        );
    }

    #[test]
    fn a_bare_read_takes_the_session_catalog_or_the_catalog_conjunct() {
        let sql = "SELECT * FROM information_schema.schemata WHERE catalog_name = 'dw'";
        assert_eq!(pinned_catalog(sql, Some("sess")), Some("sess".into()));
        assert_eq!(pinned_catalog(sql, None), Some("dw".into()));
        assert_eq!(
            pinned_catalog(
                "SELECT * FROM information_schema.tables \
                 WHERE (table_catalog = 'dw' AND table_schema = 'x')",
                None
            ),
            Some("dw".into())
        );
        assert_eq!(
            pinned_catalog(
                "SELECT * FROM information_schema.tables WHERE table_catalog IN ('dw')",
                None
            ),
            Some("dw".into())
        );
    }

    #[test]
    fn anything_wider_than_one_catalog_does_not_pin() {
        for sql in [
            "SELECT * FROM information_schema.tables",
            "SELECT * FROM information_schema.tables WHERE table_catalog IN ('a', 'b')",
            "SELECT * FROM information_schema.tables \
             WHERE table_catalog = 'a' OR table_schema = 'b'",
            "SELECT * FROM information_schema.tables WHERE table_catalog LIKE 'a%'",
            "SELECT * FROM information_schema.tables WHERE table_catalog = 'a' AND table_catalog = 'b'",
            "SELECT * FROM a.information_schema.tables JOIN b.information_schema.columns ON true",
            "SELECT * FROM a.information_schema.tables JOIN information_schema.columns ON true",
            "SELECT 1; SELECT * FROM a.information_schema.tables",
            "SELECT * FROM lake.sales.orders",
            "not sql at all",
        ] {
            assert_eq!(pinned_catalog(sql, None), None, "{sql}");
        }
    }

    #[test]
    fn schema_in_lists_and_equalities_pin_the_namespaces() {
        let pins = |sql| pinned_table_schemas_from_sql(sql);
        assert_eq!(
            pins(
                "SELECT * FROM information_schema.columns c \
                  WHERE c.table_catalog = 'dw' AND c.table_schema IN ('b', 'a')"
            ),
            Some(vec!["a".to_string(), "b".to_string()])
        );
        assert_eq!(
            pins(
                "SELECT * FROM information_schema.tables \
                  WHERE table_schema = 'a' OR (table_schema = 'b')"
            ),
            Some(vec!["a".to_string(), "b".to_string()])
        );
        assert_eq!(
            pins(
                "SELECT * FROM information_schema.tables \
                  WHERE table_schema IN ('a', 'b') AND table_schema = 'b'"
            ),
            Some(vec!["b".to_string()])
        );
        assert_eq!(
            pins("SELECT * FROM information_schema.tables WHERE 'a' = table_schema"),
            Some(vec!["a".to_string()])
        );
        for sql in [
            "SELECT * FROM information_schema.tables",
            "SELECT * FROM information_schema.tables WHERE table_schema NOT IN ('a')",
            "SELECT * FROM information_schema.tables WHERE table_schema IN ('a', table_name)",
            "SELECT * FROM information_schema.tables \
             WHERE table_schema = 'a' OR table_name = 'b'",
        ] {
            assert_eq!(pins(sql), None, "{sql}");
        }
    }
}

#[cfg(test)]
mod pinned_table_schema_tests {
    use super::pinned_table_schema_from_sql;

    #[test]
    fn a_top_level_equality_pins_the_namespace() {
        assert_eq!(
            pinned_table_schema_from_sql(
                "SELECT column_name FROM \"cat\".information_schema.columns \
                 WHERE table_schema = 'sales' AND table_name = 'orders'"
            ),
            Some("sales".to_string())
        );
        assert_eq!(
            pinned_table_schema_from_sql(
                "SELECT * FROM information_schema.tables WHERE 'hr' = c.table_schema"
            ),
            Some("hr".to_string())
        );
        assert_eq!(
            pinned_table_schema_from_sql(
                "SELECT * FROM information_schema.tables \
                 WHERE (table_catalog = 'c' AND (table_schema = 'x'))"
            ),
            Some("x".to_string())
        );
    }

    #[test]
    fn anything_wider_than_one_namespace_does_not_pin() {
        for sql in [
            "SELECT * FROM information_schema.tables",
            "SELECT * FROM information_schema.tables WHERE table_name = 'orders'",
            "SELECT * FROM information_schema.tables \
             WHERE table_schema = 'a' OR table_schema = 'b'",
            "SELECT * FROM information_schema.tables WHERE table_schema LIKE 'a%'",
            "SELECT * FROM information_schema.tables WHERE table_schema = table_name",
            "SELECT * FROM information_schema.tables WHERE table_schema <> 'a'",
            "SELECT 1; SELECT * FROM information_schema.tables WHERE table_schema = 'a'",
            "not sql at all",
        ] {
            assert_eq!(pinned_table_schema_from_sql(sql), None, "{sql}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    #[test]
    fn show_create_table_three_part_extracts_catalog() {
        let stmt = parse("SHOW CREATE TABLE disc_wh.ns.t");
        assert_eq!(extract_catalog_qualifiers(&stmt), vec!["disc_wh"]);
        assert!(extract_catalog_qualifiers(&parse("SHOW CREATE TABLE ns.t")).is_empty());
        assert!(names_polaris_system_schema(&parse(
            "SHOW CREATE TABLE system.access.audit"
        )));
        assert_eq!(
            extract_schema_qualifiers_from_sql("SHOW CREATE VIEW disc_wh.ns.v"),
            vec![(Some("disc_wh".to_string()), "ns".to_string())]
        );
    }

    #[test]
    fn polaris_system_schema_only_for_non_virtual_schemas() {
        for (sql, want) in [
            ("SELECT * FROM system.access.audit", true),
            ("SELECT * FROM SYSTEM.query.history", true),
            ("SELECT * FROM t JOIN system.lakeflow.jobs ON true", true),
            ("SELECT * FROM system.runtime.queries", false),
            ("SELECT * FROM system.JDBC.tables", false),
            ("SELECT * FROM system.metadata.catalogs", false),
            ("SELECT * FROM \"SYSTEM\".access.audit", false),
            ("SELECT * FROM iceberg.access.audit", false),
            ("SELECT * FROM access.audit", false),
        ] {
            assert_eq!(names_polaris_system_schema(&parse(sql)), want, "{sql}");
        }
    }

    #[test]
    fn rename_catalog_rewrites_three_part_relations_and_four_part_columns() {
        let mut stmt = parse(
            "SELECT POL.ns.t.id, x.y FROM pol.ns.t JOIN ns.u ON true \
             JOIN \"Pol\".ns.w ON true JOIN other.ns.t ON true \
             WHERE id IN (SELECT id FROM pol.ns.s)",
        );
        assert!(rename_catalog(&mut stmt, "pol", "test_warehouse"));
        assert_eq!(
            stmt.to_string(),
            "SELECT test_warehouse.ns.t.id, x.y FROM test_warehouse.ns.t JOIN ns.u ON true \
             JOIN \"Pol\".ns.w ON true JOIN other.ns.t ON true \
             WHERE id IN (SELECT id FROM test_warehouse.ns.s)"
        );

        let mut stmt = parse("SELECT * FROM wh.ns.t");
        assert!(rename_catalog(&mut stmt, "wh", "My-Lake"));
        assert_eq!(stmt.to_string(), "SELECT * FROM \"My-Lake\".ns.t");
        assert!(!rename_catalog(&mut stmt, "absent", "x"));
    }

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

    /// Review 806: the pre-plan refresh needs the schema of every qualified
    /// reference, with the catalog when present and without it otherwise;
    /// bare names contribute nothing.
    #[test]
    fn schema_qualifiers_cover_two_and_three_part_names() {
        let got = extract_schema_qualifiers_from_sql(
            "SELECT * FROM ns.t JOIN cat.other.u ON t.id = u.id JOIN bare ON 1=1",
        );
        assert_eq!(
            got,
            vec![
                (None, "ns".to_string()),
                (Some("cat".to_string()), "other".to_string()),
            ]
        );
        assert!(extract_schema_qualifiers_from_sql("not sql at all (").is_empty());
    }

    #[test]
    fn join_with_two_three_part_identifiers_extracts_both() {
        let stmt = parse("SELECT * FROM cat_a.ns.t1 JOIN cat_b.ns.t2 ON t1.id = t2.id");
        let mut got = extract_catalog_qualifiers(&stmt);
        got.sort();
        assert_eq!(got, vec!["cat_a".to_string(), "cat_b".to_string()]);
    }

    #[test]
    fn duplicate_qualifiers_are_deduplicated() {
        let stmt = parse("SELECT * FROM cat.ns.t1 JOIN cat.ns.t2 ON t1.id = t2.id");
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
        let stmt = parse("WITH x AS (SELECT * FROM cat.ns.t) SELECT * FROM x");
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
