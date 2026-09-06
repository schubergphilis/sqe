//! The single SQL front door (review finding 700).
//!
//! Every wire protocol used to assemble its own subset of the dialect
//! rewrites: the Trino HTTP server ran five source-level rewrites, the
//! coordinator ran one AST rewrite, the embedded CLI ran neither. The same
//! statement therefore succeeded over one protocol and failed over another,
//! and a Trino-path statement was parsed up to seven times before DataFusion
//! parsed it once more.
//!
//! [`SqlFrontDoor::normalize`] owns the ordered chain and parses once:
//!
//! 1. Tokenize once; reject multi-statement input; recognise SQE-only
//!    grammar from the token stream (this is [`crate::parse_and_classify`]'s
//!    dispatch, shared). Custom statements have no sqlparser AST and are
//!    returned already classified.
//! 2. Parse with sqlparser. Only when that fails, run the source-level
//!    fallbacks (`rewrite_paren_less_values`, `rewrite_bare_table`,
//!    `rewrite_nested_row_cast`) and parse once more.
//! 3. Run the AST rewriters on the single AST: `information_schema` catalog
//!    qualification (when a session catalog is known), the Trino-compat
//!    rewriter, and `_colN` aliasing of anonymous output columns.
//! 4. Return the AST plus the serialized text. The text is the user's own
//!    when nothing changed (sqlparser's `Display` does not reproduce every
//!    input verbatim), otherwise the re-serialized statement.
//!
//! The parse dialect is `GenericDialect`, the one every rewriter in this
//! crate is written against; the DataFusion session is separately configured
//! for its DuckDB dialect and re-parses the serialized text, exactly as
//! before.
//!
//! Callers that need the SQE clauses sqlparser cannot see (`FOR VERSION AS
//! OF`, `FOR INCREMENTAL BETWEEN`, `PARTITIONED BY (...)`) must run
//! [`crate::pre_parse_pipeline`] first: those extractors return specs the
//! coordinator needs, so they stay outside the front door.

use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::classifier::{classify_single, dispatch, Dispatch, StatementKind};
use crate::metadata::{self, UpdateType};
use crate::token_scan::parse_error;

/// The protocol a statement arrived on.
///
/// Today no rewrite is protocol-specific: Trino HTTP, Flight SQL (dbt-sqe,
/// ADBC) and the embedded CLI accept exactly the same SQL and name columns the
/// same way. `_colN` aliasing of anonymous output columns applies to all
/// three: the alternative name DataFusion produces (`Int64(1) + Int64(1)`) is
/// worse for every client, and nothing documented a reason for Flight users
/// to see a different name than Trino users for the same query. The enum
/// exists so that any future divergence is declared here, in one place, with
/// a reason, rather than by which caller remembered which rewrite.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WireDialect {
    /// Trino HTTP protocol (`/v1/statement`).
    Trino,
    /// Arrow Flight SQL (dbt-sqe, ADBC, JDBC-over-Flight).
    Flight,
    /// `sqe-cli --embedded` and other in-process callers.
    Embedded,
}

/// The one place SQL text is normalized before classification and planning.
#[derive(Debug, Clone)]
pub struct SqlFrontDoor {
    dialect: WireDialect,
    session_catalog: Option<String>,
}

impl SqlFrontDoor {
    pub fn new(dialect: WireDialect) -> Self {
        Self {
            dialect,
            session_catalog: None,
        }
    }

    /// The session's default catalog. When set, two-part
    /// `information_schema.<table>` references are qualified with it so the
    /// read resolves against the catalog the client selected (Trino
    /// `X-Trino-Catalog`, `USE catalog.schema`).
    pub fn with_session_catalog(mut self, catalog: Option<impl Into<String>>) -> Self {
        self.session_catalog = catalog.map(Into::into);
        self
    }

    pub fn dialect(&self) -> WireDialect {
        self.dialect
    }

    pub fn session_catalog(&self) -> Option<&str> {
        self.session_catalog.as_deref()
    }

    /// Normalize one statement. See the module documentation for the chain.
    ///
    /// Errors are typed `SyntaxError`s: multi-statement input, a tokenizer
    /// failure, a statement that does not parse even after the source-level
    /// fallbacks, or an SQE extension with a malformed tail.
    pub fn normalize(&self, sql: &str) -> sqe_core::Result<NormalizedSql> {
        let parsed = match dispatch(sql)? {
            Dispatch::Custom(kind) => {
                return Ok(NormalizedSql {
                    sql: sql.to_string(),
                    statements: Vec::new(),
                    custom: Some(kind),
                    source_rewritten: false,
                    ast_rewritten: false,
                });
            }
            Dispatch::Standard(parsed) => parsed,
        };

        let (mut statements, sql, source_rewritten) = match parsed {
            Ok(statements) if statements.is_empty() => {
                return Err(crate::token_scan::syntax_error("Empty SQL"));
            }
            Ok(statements) => (statements, sql.to_string(), false),
            Err(first_err) => {
                // Source-level fallbacks for Trino shapes sqlparser rejects.
                // Each is parse-gated internally, so they compose; the order
                // is the one the Trino HTTP path used.
                let rewritten = crate::rewrite_paren_less_values(sql);
                let rewritten = crate::rewrite_bare_table(&rewritten);
                let rewritten = crate::rewrite_nested_row_cast(&rewritten);
                if rewritten == sql {
                    return Err(parse_error(first_err));
                }
                let statements =
                    Parser::parse_sql(&GenericDialect {}, &rewritten).map_err(parse_error)?;
                (statements, rewritten, true)
            }
        };

        let mut ast_rewritten = false;
        if let Some(catalog) = self.session_catalog.as_deref() {
            for stmt in statements.iter_mut() {
                ast_rewritten |= metadata::qualify_information_schema(stmt, catalog);
            }
        }
        ast_rewritten |= crate::trino_compat::apply_trino_compat(&mut statements, &sql);
        ast_rewritten |= crate::trino_compat::apply_anonymous_column_aliases(&mut statements);

        let sql = if ast_rewritten {
            statements
                .iter()
                .map(Statement::to_string)
                .collect::<Vec<_>>()
                .join("; ")
        } else {
            sql
        };

        Ok(NormalizedSql {
            sql,
            statements,
            custom: None,
            source_rewritten,
            ast_rewritten,
        })
    }
}

/// A statement after the front door: the text to hand to DataFusion and the
/// AST it was produced from.
#[derive(Debug)]
pub struct NormalizedSql {
    sql: String,
    statements: Vec<Statement>,
    /// Set for SQE-only grammar, which sqlparser cannot parse; the statement
    /// is already classified and `statements` is empty.
    custom: Option<StatementKind>,
    source_rewritten: bool,
    ast_rewritten: bool,
}

impl NormalizedSql {
    /// The SQL text to execute: the user's own text when nothing changed,
    /// otherwise the re-serialized statement.
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// The parsed (and rewritten) statements. Empty for SQE-only grammar.
    pub fn statements(&self) -> &[Statement] {
        &self.statements
    }

    /// The single parsed statement, when sqlparser produced one.
    pub fn statement(&self) -> Option<&Statement> {
        self.statements.first()
    }

    /// True for SQE-only grammar that never had a sqlparser AST.
    pub fn is_custom_grammar(&self) -> bool {
        self.custom.is_some()
    }

    /// True when a source-level fallback rewrote the text before parsing.
    pub fn source_rewritten(&self) -> bool {
        self.source_rewritten
    }

    /// True when an AST rewriter changed the statement (the text is then
    /// re-serialized).
    pub fn ast_rewritten(&self) -> bool {
        self.ast_rewritten
    }

    /// True when the statement reads `information_schema` (any depth).
    /// Always false for SQE-only grammar.
    pub fn reads_information_schema(&self) -> bool {
        self.statement()
            .is_some_and(metadata::reads_information_schema)
    }

    /// True when the Trino layer should reshape the result as catalog
    /// metadata (`information_schema` reads, `SHOW COLUMNS`, `DESCRIBE`).
    pub fn is_metadata_query(&self) -> bool {
        self.statement().is_some_and(metadata::is_metadata_query)
    }

    /// The Trino `updateType`, or `None` for reads.
    pub fn update_type(&self) -> Option<UpdateType> {
        match &self.custom {
            Some(kind) => kind.update_type(),
            None => self.statement().and_then(metadata::update_type),
        }
    }

    /// Consume into `(sql, statements)`.
    pub fn into_parts(self) -> (String, Vec<Statement>) {
        (self.sql, self.statements)
    }
}

/// Classify a normalized statement without re-tokenizing. SQE-only grammar
/// was classified during dispatch; standard statements are classified from
/// the AST the front door already holds.
pub fn classify_normalized(normalized: NormalizedSql) -> sqe_core::Result<StatementKind> {
    match normalized.custom {
        Some(kind) => Ok(kind),
        None => classify_single(normalized.statements),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqe_core::SqeErrorCode;

    const DIALECTS: [WireDialect; 3] = [
        WireDialect::Trino,
        WireDialect::Flight,
        WireDialect::Embedded,
    ];

    /// Trino-shape statements that only worked over the Trino HTTP door
    /// before the front door existed, plus plain ones. Every dialect must
    /// produce byte-identical output.
    const TRINO_SHAPES: &[&str] = &[
        "INSERT INTO t VALUES 1",
        "INSERT INTO t VALUES 1, 2, 3",
        "TABLE nation",
        "SELECT CAST(ROW(1, 'a') AS ROW(x INTEGER, y VARCHAR))",
        "SELECT 1+1",
        "SELECT a, count(*) FROM t GROUP BY a",
        "SELECT * FROM t WHERE x = 1",
        "SELECT CAST(j AS JSON) FROM t",
        "SELECT * FROM t FETCH FIRST 5 ROWS ONLY",
        "SELECT * FROM information_schema.tables",
        "SHOW TABLES",
        "CREATE TABLE t (id INT)",
        "SELECT 'information_schema.tables'",
    ];

    #[test]
    fn every_dialect_normalizes_trino_shapes_identically() {
        for sql in TRINO_SHAPES {
            let outputs: Vec<String> = DIALECTS
                .iter()
                .map(|d| {
                    SqlFrontDoor::new(*d)
                        .with_session_catalog(Some("iceberg"))
                        .normalize(sql)
                        .unwrap_or_else(|e| panic!("{sql} via {d:?}: {e}"))
                        .sql()
                        .to_string()
                })
                .collect();
            assert!(
                outputs.windows(2).all(|w| w[0] == w[1]),
                "{sql}: dialects disagree: {outputs:?}"
            );
        }
    }

    #[test]
    fn source_fallbacks_run_only_when_the_first_parse_fails() {
        let door = SqlFrontDoor::new(WireDialect::Flight);
        let n = door.normalize("INSERT INTO t VALUES 1").unwrap();
        assert!(n.source_rewritten());
        assert_eq!(n.sql(), "INSERT INTO t VALUES (1)");
        assert!(matches!(n.statement(), Some(Statement::Insert(_))));

        let n = door.normalize("TABLE nation").unwrap();
        assert!(n.source_rewritten());
        assert!(matches!(n.statement(), Some(Statement::Query(_))));

        let n = door.normalize("SELECT * FROM t WHERE x = 1").unwrap();
        assert!(!n.source_rewritten());
        assert!(!n.ast_rewritten());
        assert_eq!(n.sql(), "SELECT * FROM t WHERE x = 1");
    }

    #[test]
    fn anonymous_columns_get_trino_names_on_every_dialect() {
        for d in DIALECTS {
            let n = SqlFrontDoor::new(d).normalize("SELECT 1+1").unwrap();
            assert!(n.ast_rewritten(), "{d:?}");
            assert_eq!(n.sql(), "SELECT 1 + 1 AS _col0", "{d:?}");
        }
    }

    #[test]
    fn information_schema_is_qualified_with_the_session_catalog_only_in_relations() {
        let door = SqlFrontDoor::new(WireDialect::Trino).with_session_catalog(Some("cat"));
        let n = door
            .normalize("SELECT table_name FROM information_schema.tables")
            .unwrap();
        assert!(n.reads_information_schema());
        assert!(n.is_metadata_query());
        assert_eq!(
            n.sql(),
            "SELECT table_name FROM \"cat\".information_schema.tables"
        );

        let n = door
            .normalize("SELECT 'information_schema.tables'")
            .unwrap();
        assert!(!n.reads_information_schema());
        // The literal is not a relation, and the anonymous column gets its
        // Trino name; the literal itself is untouched.
        assert_eq!(n.sql(), "SELECT 'information_schema.tables' AS _col0");

        let n = door
            .normalize("SELECT x FROM t -- information_schema.tables")
            .unwrap();
        assert!(!n.reads_information_schema());
        assert_eq!(n.sql(), "SELECT x FROM t -- information_schema.tables");
    }

    #[test]
    fn no_session_catalog_means_no_qualification() {
        let n = SqlFrontDoor::new(WireDialect::Flight)
            .normalize("SELECT table_name FROM information_schema.tables")
            .unwrap();
        assert!(n.reads_information_schema());
        assert_eq!(n.sql(), "SELECT table_name FROM information_schema.tables");
    }

    #[test]
    fn multi_statement_input_is_a_syntax_error() {
        let err = SqlFrontDoor::new(WireDialect::Trino)
            .normalize("CREATE TABLE t (id INT); INSERT INTO t VALUES (1)")
            .unwrap_err();
        assert_eq!(err.error_code(), SqeErrorCode::SyntaxError);
        assert!(err.to_string().contains("line 1, column 26"), "{err}");
    }

    #[test]
    fn unparseable_input_is_a_sourced_syntax_error() {
        let err = SqlFrontDoor::new(WireDialect::Embedded)
            .normalize("SELEC 1")
            .unwrap_err();
        assert_eq!(err.error_code(), SqeErrorCode::SyntaxError);
        assert!(
            std::error::Error::source(&err).is_some(),
            "parser error must be kept as source"
        );
    }

    #[test]
    fn custom_grammar_is_classified_during_dispatch() {
        let n = SqlFrontDoor::new(WireDialect::Trino)
            .normalize("-- leading comment\nSHOW TAGS ON ns.t")
            .unwrap();
        assert!(n.is_custom_grammar());
        assert!(n.statements().is_empty());
        assert_eq!(n.update_type(), None);
        assert!(matches!(
            classify_normalized(n).unwrap(),
            StatementKind::ShowTags(ref t) if t == "ns.t"
        ));
    }

    #[test]
    fn classify_normalized_reuses_the_ast() {
        let n = SqlFrontDoor::new(WireDialect::Flight)
            .normalize("CREATE VIEW v AS SELECT 1")
            .unwrap();
        assert_eq!(n.update_type(), Some(UpdateType::CreateView));
        assert!(matches!(
            classify_normalized(n).unwrap(),
            StatementKind::CreateView(_)
        ));

        let n = SqlFrontDoor::new(WireDialect::Flight)
            .normalize("INSERT INTO t VALUES 1")
            .unwrap();
        assert_eq!(n.update_type(), Some(UpdateType::Insert));
        assert!(matches!(
            classify_normalized(n).unwrap(),
            StatementKind::Insert(_)
        ));
    }

    #[test]
    fn empty_input_is_a_syntax_error() {
        let err = SqlFrontDoor::new(WireDialect::Flight)
            .normalize("   ")
            .unwrap_err();
        assert_eq!(err.error_code(), SqeErrorCode::SyntaxError);
    }
}
