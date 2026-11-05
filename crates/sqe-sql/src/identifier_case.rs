//! Per-catalog identifier case: unquoted names fold to upper case for the
//! catalogs that ask for it (`identifier_case = "upper"`).
//!
//! SQE folds unquoted identifiers to lower case and then matches names
//! exactly (#337, DataFusion's ident normalization). Snowflake folds them to
//! upper case, so a table Snowflake created with `CREATE ICEBERG TABLE t (id
//! INT)` is `T` with a column `ID` in its Iceberg schema, and SQE could only
//! reach it as `"T"` and `"ID"`. For a catalog marked upper-case the front
//! door rewrites the unquoted names to the quoted upper-case form a user could
//! already type. The output is quoted on purpose: DataFusion keeps a quoted
//! name as written, and a second pass over the rewritten text changes
//! nothing.
//!
//! The rewrite is a fold, not a lookup. `id` means `ID` in an upper-case
//! catalog whether or not a column `id` also exists, exactly as in Snowflake;
//! a lower-case name there (a table SQE created with a quoted lower-case
//! name) is reached quoted, as Snowflake reaches it.
//!
//! Scope:
//!
//! - Table names (schema and table parts, never the catalog part) fold
//!   whenever the table lives in an upper-case catalog: FROM, JOIN, INSERT,
//!   UPDATE, DELETE, MERGE, CREATE TABLE and DROP targets. A one- or two-part
//!   name belongs to the session catalog. `information_schema` and metadata
//!   tables keep their schema name, and the `$snapshots` suffix stays lower
//!   case.
//! - Column names, aliases and CTE names fold only when every table the
//!   statement reads or writes is in an upper-case catalog. A statement that
//!   also touches another catalog, a metadata table or a table function
//!   folds table names only: which side an unqualified `id` belongs to is not
//!   decidable without the schemas, so the user quotes the column.
//! - Function and type names never fold.
//!
//! Policies bind to the names in the plan, and Ranger matches table, column
//! and mask names without case, so a rewritten statement is governed exactly
//! as the same statement quoted by hand.

use std::collections::HashSet;
use std::ops::ControlFlow;

use sqlparser::ast::{
    AssignmentTarget, Expr, Ident, JoinConstraint, JoinOperator, MergeAction, MergeClause,
    ObjectName, ObjectNamePart, Query, Select, SelectItem, SelectItemQualifiedWildcardKind,
    Statement, TableAlias, TableFactor, TableObject, Use, VisitMut, VisitorMut,
};

/// The catalogs whose unquoted identifiers fold to upper case, by the name
/// the engine registered them under.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UpperCaseCatalogs {
    names: HashSet<String>,
    /// The catalog a one- or two-part name resolves against when the session
    /// names none (the engine's default catalog).
    default_catalog: Option<String>,
}

impl UpperCaseCatalogs {
    pub fn new(names: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            names: names.into_iter().map(Into::into).collect(),
            default_catalog: None,
        }
    }

    /// The engine's default catalog, for statements whose session names no
    /// catalog.
    pub fn with_default_catalog(mut self, catalog: Option<String>) -> Self {
        self.default_catalog = catalog;
        self
    }

    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }

    pub fn contains(&self, catalog: &str) -> bool {
        self.names.contains(catalog)
    }

    pub fn insert(&mut self, catalog: impl Into<String>) {
        self.names.insert(catalog.into());
    }

    pub fn remove(&mut self, catalog: &str) {
        self.names.remove(catalog);
    }
}

/// Fold the unquoted identifiers of `statements` that name objects in an
/// upper-case catalog. `session_catalog` is the catalog one- and two-part
/// table names resolve against (the engine default when `None`). Returns true
/// when anything changed.
pub fn fold_upper_case_identifiers(
    statements: &mut [Statement],
    catalogs: &UpperCaseCatalogs,
    session_catalog: Option<&str>,
) -> bool {
    if catalogs.is_empty() {
        return false;
    }
    let session_catalog = session_catalog.or(catalogs.default_catalog.as_deref());
    let mut changed = false;
    for stmt in statements.iter_mut() {
        if let Statement::Use(target) = stmt {
            changed |= fold_use_target(target, catalogs, session_catalog);
            continue;
        }
        if !matches!(
            stmt,
            Statement::Query(_)
                | Statement::Insert(_)
                | Statement::Update(_)
                | Statement::Delete(_)
                | Statement::Merge(_)
                | Statement::CreateTable(_)
                | Statement::CreateView(_)
                | Statement::Drop { .. }
        ) {
            continue;
        }
        let mut survey = Folder::new(catalogs, session_catalog, Pass::Survey);
        let _ = stmt.visit(&mut survey);
        let scope = survey.scope();
        if scope == Scope::Nothing {
            continue;
        }
        let mut folder = Folder::new(catalogs, session_catalog, Pass::Fold(scope));
        folder.ctes = survey.ctes;
        let _ = stmt.visit(&mut folder);
        changed |= folder.changed;
    }
    changed
}

/// `USE sf.sales` / `USE sales` (session catalog `sf`) sets the schema that
/// one-part names resolve in, so its schema part folds like a table name's:
/// `USE sf."SALES"`. Without it the session would hold `sales`, and every
/// later `SELECT .. FROM orders` would look for `sf.sales.ORDERS`.
/// `USE CATALOG` names a catalog, which never folds.
fn fold_use_target(
    target: &mut Use,
    catalogs: &UpperCaseCatalogs,
    session_catalog: Option<&str>,
) -> bool {
    let name = match target {
        Use::Object(name) | Use::Schema(name) | Use::Database(name) => name,
        _ => return false,
    };
    let Some(parts) = identifiers(name) else {
        return false;
    };
    let catalog = match parts.len() {
        2 => Some(resolve(&parts[0])),
        1 => session_catalog.map(str::to_string),
        _ => None,
    };
    if !catalog.is_some_and(|c| catalogs.contains(&c)) {
        return false;
    }
    if parts[parts.len() - 1]
        .value
        .eq_ignore_ascii_case("information_schema")
    {
        return false;
    }
    match name.0.last_mut() {
        Some(ObjectNamePart::Identifier(schema)) => fold_ident(schema),
        _ => false,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Scope {
    /// No table in an upper-case catalog.
    Nothing,
    /// Fold the names of tables in upper-case catalogs only.
    TableNames,
    /// Every table is in an upper-case catalog: fold columns, aliases and
    /// CTE names too.
    Everything,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Pass {
    Survey,
    Fold(Scope),
}

/// How one table name relates to the upper-case catalogs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Relation {
    /// In an upper-case catalog, with upper-case columns.
    Upper,
    /// In an upper-case catalog, but its columns are SQE's own lower-case
    /// names (`information_schema`, a `$snapshots` metadata table). The table
    /// part may still fold.
    UpperNameLowerColumns,
    /// Not in an upper-case catalog.
    Lower,
}

struct Folder<'a> {
    catalogs: &'a UpperCaseCatalogs,
    session_catalog: Option<&'a str>,
    pass: Pass,
    /// CTE names (as written, unquoted ones lower-cased): a one-part name
    /// that matches one is the CTE, not a table.
    ctes: HashSet<String>,
    upper: usize,
    lower: usize,
    changed: bool,
}

impl<'a> Folder<'a> {
    fn new(catalogs: &'a UpperCaseCatalogs, session_catalog: Option<&'a str>, pass: Pass) -> Self {
        Self {
            catalogs,
            session_catalog,
            pass,
            ctes: HashSet::new(),
            upper: 0,
            lower: 0,
            changed: false,
        }
    }

    fn scope(&self) -> Scope {
        match (self.upper, self.lower) {
            (0, _) => Scope::Nothing,
            (_, 0) => Scope::Everything,
            _ => Scope::TableNames,
        }
    }

    fn folds_everything(&self) -> bool {
        self.pass == Pass::Fold(Scope::Everything)
    }

    fn fold(&mut self, ident: &mut Ident) {
        if fold_ident(ident) {
            self.changed = true;
        }
    }

    /// Fold a column, alias or CTE name when the whole statement is upper.
    fn fold_name(&mut self, ident: &mut Ident) {
        if self.folds_everything() {
            self.fold(ident);
        }
    }

    fn fold_alias(&mut self, alias: &mut Option<TableAlias>) {
        if let Some(alias) = alias {
            self.fold_name(&mut alias.name);
            for column in &mut alias.columns {
                self.fold_name(&mut column.name);
            }
        }
    }

    /// Fold every part of a column reference written as an object name
    /// (`INSERT (a, b)`, `SET t.a = ..`, `USING (a)`, `t.*`). Four or more
    /// parts start with a catalog, which is left alone.
    fn fold_column_path(&mut self, name: &mut ObjectName) {
        if !self.folds_everything() {
            return;
        }
        let skip = usize::from(name.0.len() >= 4);
        for part in name.0.iter_mut().skip(skip) {
            if let ObjectNamePart::Identifier(ident) = part {
                self.fold(ident);
            }
        }
    }

    /// `t.*`, `s.t.*`: a three-part qualifier starts with a catalog.
    fn fold_wildcard_qualifier(&mut self, name: &mut ObjectName) {
        if !self.folds_everything() {
            return;
        }
        let skip = usize::from(name.0.len() >= 3);
        for part in name.0.iter_mut().skip(skip) {
            if let ObjectNamePart::Identifier(ident) = part {
                self.fold(ident);
            }
        }
    }

    /// Classify a table name; in the fold pass, fold it when it is in an
    /// upper-case catalog.
    fn table(&mut self, name: &mut ObjectName) {
        let Some(parts) = identifiers(name) else {
            self.lower += 1;
            return;
        };
        if parts.len() == 1 && self.ctes.contains(&resolve(&parts[0])) {
            // A CTE reference: folds with the CTE's own name.
            if let ObjectNamePart::Identifier(ident) = &mut name.0[0] {
                self.fold_name(ident);
            }
            return;
        }
        let relation = self.classify(&parts);
        match self.pass {
            Pass::Survey => match relation {
                Relation::Upper => self.upper += 1,
                Relation::UpperNameLowerColumns => {
                    self.upper += 1;
                    self.lower += 1;
                }
                Relation::Lower => self.lower += 1,
            },
            Pass::Fold(_) => {
                if relation != Relation::Lower {
                    self.fold_table_name(name);
                }
            }
        }
    }

    fn classify(&self, parts: &[Ident]) -> Relation {
        let catalog = match parts.len() {
            3 => Some(resolve(&parts[0])),
            1 | 2 => self.session_catalog.map(str::to_string),
            _ => None,
        };
        if !catalog.is_some_and(|c| self.catalogs.contains(&c)) {
            return Relation::Lower;
        }
        let schema_is_metadata = parts.len() >= 2
            && parts[parts.len() - 2]
                .value
                .eq_ignore_ascii_case("information_schema");
        let table_is_metadata = parts[parts.len() - 1].value.contains('$');
        if schema_is_metadata || table_is_metadata {
            Relation::UpperNameLowerColumns
        } else {
            Relation::Upper
        }
    }

    /// Fold the schema and table parts. `information_schema` keeps its name,
    /// and a metadata suffix (`t$snapshots`) stays as written.
    fn fold_table_name(&mut self, name: &mut ObjectName) {
        let is_information_schema = name.0.iter().any(|part| {
            matches!(part, ObjectNamePart::Identifier(i) if i.value.eq_ignore_ascii_case("information_schema"))
        });
        if is_information_schema {
            return;
        }
        let first = name.0.len().saturating_sub(2);
        for part in name.0.iter_mut().skip(first) {
            let ObjectNamePart::Identifier(ident) = part else {
                continue;
            };
            if ident.quote_style.is_some() {
                continue;
            }
            let value = match ident.value.split_once('$') {
                Some((table, suffix)) => {
                    format!(
                        "{}${}",
                        table.to_ascii_uppercase(),
                        suffix.to_ascii_lowercase()
                    )
                }
                None => ident.value.to_ascii_uppercase(),
            };
            *ident = Ident::with_quote('"', value);
            self.changed = true;
        }
    }
}

impl VisitorMut for Folder<'_> {
    type Break = ();

    fn pre_visit_statement(&mut self, statement: &mut Statement) -> ControlFlow<()> {
        match statement {
            Statement::Insert(insert) => {
                match &mut insert.table {
                    TableObject::TableName(name) => self.table(name),
                    TableObject::TableFunction(_) | TableObject::TableQuery(_) => self.lower += 1,
                }
                for column in &mut insert.columns {
                    self.fold_column_path(column);
                }
            }
            Statement::Update(update) => {
                for assignment in &mut update.assignments {
                    match &mut assignment.target {
                        AssignmentTarget::ColumnName(name) => self.fold_column_path(name),
                        AssignmentTarget::Tuple(names) => {
                            for name in names {
                                self.fold_column_path(name);
                            }
                        }
                    }
                }
            }
            Statement::Merge(merge) => {
                for clause in &mut merge.clauses {
                    self.merge_clause(clause);
                }
            }
            Statement::CreateTable(create) => {
                self.table(&mut create.name);
                for column in &mut create.columns {
                    self.fold_name(&mut column.name);
                }
            }
            Statement::CreateView(view) => {
                self.table(&mut view.name);
            }
            Statement::Drop { names, .. } => {
                for name in names {
                    self.table(name);
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<()> {
        if let Some(with) = &mut query.with {
            for cte in &mut with.cte_tables {
                if self.pass == Pass::Survey {
                    self.ctes.insert(resolve(&cte.alias.name));
                }
                self.fold_name(&mut cte.alias.name);
                for column in &mut cte.alias.columns {
                    self.fold_name(&mut column.name);
                }
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_select(&mut self, select: &mut Select) -> ControlFlow<()> {
        for item in &mut select.projection {
            match item {
                SelectItem::ExprWithAlias { alias, .. } => self.fold_name(alias),
                SelectItem::QualifiedWildcard(
                    SelectItemQualifiedWildcardKind::ObjectName(name),
                    _,
                ) => self.fold_wildcard_qualifier(name),
                _ => {}
            }
        }
        for from in &mut select.from {
            for join in &mut from.joins {
                if let Some(JoinConstraint::Using(columns)) =
                    join_constraint(&mut join.join_operator)
                {
                    for column in columns {
                        self.fold_column_path(column);
                    }
                }
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, factor: &mut TableFactor) -> ControlFlow<()> {
        match factor {
            TableFactor::Table {
                name, alias, args, ..
            } => {
                if args.is_some() {
                    // A table function: its columns are its own.
                    self.lower += usize::from(self.pass == Pass::Survey);
                } else {
                    self.table(name);
                }
                self.fold_alias(alias);
            }
            TableFactor::Derived { alias, .. } | TableFactor::NestedJoin { alias, .. } => {
                self.fold_alias(alias);
            }
            TableFactor::TableFunction { alias, .. }
            | TableFactor::Function { alias, .. }
            | TableFactor::UNNEST { alias, .. } => {
                // Columns of a function's own making.
                self.lower += usize::from(self.pass == Pass::Survey);
                self.fold_alias(alias);
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        if !self.folds_everything() {
            return ControlFlow::Continue(());
        }
        match expr {
            Expr::Identifier(ident) => self.fold(ident),
            Expr::CompoundIdentifier(parts) => {
                let skip = usize::from(parts.len() >= 4);
                for ident in parts.iter_mut().skip(skip) {
                    self.fold(ident);
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

impl Folder<'_> {
    fn merge_clause(&mut self, clause: &mut MergeClause) {
        match &mut clause.action {
            MergeAction::Insert(insert) => {
                for column in &mut insert.columns {
                    self.fold_column_path(column);
                }
            }
            MergeAction::Update(update) => {
                for assignment in &mut update.assignments {
                    match &mut assignment.target {
                        AssignmentTarget::ColumnName(name) => self.fold_column_path(name),
                        AssignmentTarget::Tuple(names) => {
                            for name in names {
                                self.fold_column_path(name);
                            }
                        }
                    }
                }
            }
            MergeAction::Delete { .. } => {}
        }
    }
}

fn join_constraint(op: &mut JoinOperator) -> Option<&mut JoinConstraint> {
    match op {
        JoinOperator::Join(c)
        | JoinOperator::Inner(c)
        | JoinOperator::Left(c)
        | JoinOperator::LeftOuter(c)
        | JoinOperator::Right(c)
        | JoinOperator::RightOuter(c)
        | JoinOperator::FullOuter(c)
        | JoinOperator::Semi(c)
        | JoinOperator::LeftSemi(c)
        | JoinOperator::RightSemi(c)
        | JoinOperator::Anti(c)
        | JoinOperator::LeftAnti(c)
        | JoinOperator::RightAnti(c) => Some(c),
        _ => None,
    }
}

/// The plain identifier parts of a name, or `None` when a part is a
/// dialect-specific function.
fn identifiers(name: &ObjectName) -> Option<Vec<Ident>> {
    name.0
        .iter()
        .map(|part| match part {
            ObjectNamePart::Identifier(ident) => Some(ident.clone()),
            ObjectNamePart::Function(_) => None,
        })
        .collect()
}

/// The name DataFusion resolves an identifier to: quoted as written,
/// unquoted lower-cased (ASCII only, as DataFusion folds).
fn resolve(ident: &Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_ascii_lowercase()
    }
}

/// Upper-case and double-quote an unquoted identifier. Returns true when it
/// changed.
fn fold_ident(ident: &mut Ident) -> bool {
    if ident.quote_style.is_some() {
        return false;
    }
    *ident = Ident::with_quote('"', ident.value.to_ascii_uppercase());
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    fn fold(sql: &str, session: Option<&str>) -> String {
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
        fold_upper_case_identifiers(&mut statements, &UpperCaseCatalogs::new(["sf"]), session);
        statements[0].to_string()
    }

    #[test]
    fn folds_tables_and_columns_of_an_upper_case_catalog() {
        assert_eq!(
            fold("SELECT id, region AS r FROM sf.sqe_probe.sf_run_basic WHERE region = 'eu' ORDER BY r", None),
            r#"SELECT "ID", "REGION" AS "R" FROM sf."SQE_PROBE"."SF_RUN_BASIC" WHERE "REGION" = 'eu' ORDER BY "R""#
        );
    }

    #[test]
    fn leaves_other_catalogs_alone() {
        let sql = "SELECT id FROM iceberg.s.t WHERE id = 1";
        assert_eq!(fold(sql, None), sql);
        assert_eq!(fold(sql, Some("sf")), sql);
    }

    #[test]
    fn quoted_names_keep_their_case() {
        assert_eq!(
            fold(r#"SELECT "id", x FROM sf."sqe_probe"."sqe_basic""#, None),
            r#"SELECT "id", "X" FROM sf."sqe_probe"."sqe_basic""#
        );
    }

    #[test]
    fn session_catalog_resolves_short_names() {
        assert_eq!(
            fold("SELECT id FROM s.t", Some("sf")),
            r#"SELECT "ID" FROM "S"."T""#
        );
        assert_eq!(
            fold("SELECT id FROM s.t", Some("iceberg")),
            "SELECT id FROM s.t"
        );
    }

    #[test]
    fn use_folds_the_schema_of_an_upper_case_catalog() {
        assert_eq!(fold("USE sf.sqe_probe", None), r#"USE sf."SQE_PROBE""#);
        assert_eq!(fold("USE sqe_probe", Some("sf")), r#"USE "SQE_PROBE""#);
        assert_eq!(fold(r#"USE sf."lower""#, None), r#"USE sf."lower""#);
        assert_eq!(
            fold("USE sf.information_schema", None),
            "USE sf.information_schema"
        );
        assert_eq!(fold("USE iceberg.s", None), "USE iceberg.s");
        assert_eq!(fold("USE sqe_probe", Some("iceberg")), "USE sqe_probe");
    }

    #[test]
    fn engine_default_catalog_resolves_short_names_without_a_session_catalog() {
        let catalogs = UpperCaseCatalogs::new(["sf"]).with_default_catalog(Some("sf".into()));
        let mut statements = Parser::parse_sql(&GenericDialect {}, "SELECT id FROM s.t").unwrap();
        assert!(fold_upper_case_identifiers(
            &mut statements,
            &catalogs,
            None
        ));
        assert_eq!(statements[0].to_string(), r#"SELECT "ID" FROM "S"."T""#);
        // The session's own catalog wins.
        let mut statements = Parser::parse_sql(&GenericDialect {}, "SELECT id FROM s.t").unwrap();
        assert!(!fold_upper_case_identifiers(
            &mut statements,
            &catalogs,
            Some("iceberg")
        ));
    }

    #[test]
    fn functions_and_types_never_fold() {
        assert_eq!(
            fold(
                "SELECT count(*), sum(CAST(amount AS BIGINT)) FROM sf.s.t",
                None
            ),
            r#"SELECT count(*), sum(CAST("AMOUNT" AS BIGINT)) FROM sf."S"."T""#
        );
    }

    #[test]
    fn mixed_catalogs_fold_table_names_only() {
        assert_eq!(
            fold(
                "SELECT a.id, b.id FROM sf.s.t a JOIN iceberg.s.u b ON a.id = b.id",
                None
            ),
            r#"SELECT a.id, b.id FROM sf."S"."T" a JOIN iceberg.s.u b ON a.id = b.id"#
        );
    }

    #[test]
    fn ctes_and_aliases_fold_consistently() {
        assert_eq!(
            fold(
                "WITH c AS (SELECT id FROM sf.s.t) SELECT c.id FROM c JOIN sf.s.u x USING (id)",
                None
            ),
            r#"WITH "C" AS (SELECT "ID" FROM sf."S"."T") SELECT "C"."ID" FROM "C" JOIN sf."S"."U" "X" USING("ID")"#
        );
    }

    #[test]
    fn metadata_tables_keep_lower_case_columns() {
        assert_eq!(
            fold("SELECT snapshot_id FROM sf.s.t$snapshots", None),
            r#"SELECT snapshot_id FROM sf."S"."T$snapshots""#
        );
        assert_eq!(
            fold("SELECT table_name FROM sf.information_schema.tables", None),
            "SELECT table_name FROM sf.information_schema.tables"
        );
    }

    #[test]
    fn writes_fold_target_and_columns() {
        assert_eq!(
            fold("INSERT INTO sf.s.t (id, region) VALUES (1, 'eu')", None),
            r#"INSERT INTO sf."S"."T" ("ID", "REGION") VALUES (1, 'eu')"#
        );
        assert_eq!(
            fold("UPDATE sf.s.t SET region = 'us' WHERE id = 1", None),
            r#"UPDATE sf."S"."T" SET "REGION" = 'us' WHERE "ID" = 1"#
        );
        assert_eq!(
            fold("DELETE FROM sf.s.t WHERE id = 1", None),
            r#"DELETE FROM sf."S"."T" WHERE "ID" = 1"#
        );
        assert_eq!(
            fold("CREATE TABLE sf.s.n (id INT, name VARCHAR)", None),
            r#"CREATE TABLE sf."S"."N" ("ID" INT, "NAME" VARCHAR)"#
        );
    }

    #[test]
    fn merge_folds_target_source_and_clauses() {
        assert_eq!(
            fold(
                "MERGE INTO sf.s.t t USING sf.s.u s ON t.id = s.id \
                 WHEN MATCHED THEN UPDATE SET region = s.region \
                 WHEN NOT MATCHED THEN INSERT (id, region) VALUES (s.id, s.region)",
                None
            ),
            r#"MERGE INTO sf."S"."T" "T" USING sf."S"."U" "S" ON "T"."ID" = "S"."ID" WHEN MATCHED THEN UPDATE SET "REGION" = "S"."REGION" WHEN NOT MATCHED THEN INSERT ("ID", "REGION") VALUES ("S"."ID", "S"."REGION")"#
        );
    }

    #[test]
    fn folding_twice_changes_nothing() {
        let once = fold("SELECT id FROM sf.s.t WHERE id > 1", None);
        assert_eq!(fold(&once, None), once);
    }

    #[test]
    fn no_upper_case_catalog_is_a_no_op() {
        let mut statements =
            Parser::parse_sql(&GenericDialect {}, "SELECT id FROM sf.s.t").unwrap();
        assert!(!fold_upper_case_identifiers(
            &mut statements,
            &UpperCaseCatalogs::default(),
            None
        ));
    }
}
