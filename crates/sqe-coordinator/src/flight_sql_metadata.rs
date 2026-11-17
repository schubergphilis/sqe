//! Flight SQL catalog metadata: the `SqlInfo` table and the LIKE matcher the
//! `GetDbSchemas` / `GetTables` filters use.

use arrow_flight::sql::metadata::SqlInfoDataBuilder;
use tonic::Status;

/// Populate the full Flight SQL `SqlInfo` table.
///
/// Apache ADBC, Tableau's Flight SQL connector, and dbt-sqe read these
/// keys to decide which catalog APIs to call and which SQL grammar to
/// emit. Absent keys silently degrade clients (Tableau falls back to no
/// identifier quoting; JDBC `DatabaseMetaData.getIdentifierQuoteString`
/// returns `""`). Apache Arrow Flight SQL spec section
/// `CommandGetSqlInfo` enumerates the standard set.
pub fn build_sql_info_data() -> Result<arrow_flight::sql::metadata::SqlInfoData, Status> {
    use arrow_flight::sql::SqlInfo as I;

    let mut b = SqlInfoDataBuilder::new();

    // Server identity.
    b.append(I::FlightSqlServerName, "SQE Coordinator");
    b.append(I::FlightSqlServerVersion, env!("CARGO_PKG_VERSION"));
    b.append(I::FlightSqlServerArrowVersion, "1.3");

    // Server capabilities. SQE accepts writes (CTAS/INSERT/UPDATE/DELETE).
    b.append(I::FlightSqlServerReadOnly, false);
    b.append(I::FlightSqlServerSql, true);
    b.append(I::FlightSqlServerSubstrait, false);
    // SqlSupportedTransaction.TRANSACTION = 1: BeginTransaction /
    // EndTransaction, no savepoints (#824). Advertising it is what makes a
    // DB-API driver run `autocommit=False` connections in a real transaction
    // instead of falling back to autocommit.
    b.append(I::FlightSqlServerTransaction, 1i32);
    b.append(I::FlightSqlServerCancel, true);
    b.append(I::FlightSqlServerBulkIngestion, false);
    // An ingest carrying a transaction id writes to the transaction's branch.
    b.append(I::FlightSqlServerIngestTransactionsSupported, true);

    // DDL support.
    b.append(I::SqlDdlCatalog, false);
    b.append(I::SqlDdlSchema, true);
    b.append(I::SqlDdlTable, true);

    // Identifier handling. Bare identifiers fold to lower case
    // (SqlSupportedCaseSensitivity LOWERCASE = 3). Quoted ones keep their
    // case, which the enum (unknown, insensitive, upper, lower) cannot say:
    // UNKNOWN = 0.
    b.append(I::SqlIdentifierCase, 3i32);
    b.append(I::SqlIdentifierQuoteChar, "\"");
    b.append(I::SqlQuotedIdentifierCase, 0i32);

    // Catalog and schema semantics.
    b.append(I::SqlAllTablesAreSelectable, true);
    b.append(I::SqlCatalogTerm, "catalog");
    b.append(I::SqlSchemaTerm, "schema");
    b.append(I::SqlProcedureTerm, "procedure");
    b.append(I::SqlCatalogAtStart, true);
    // SqlNullOrdering.SQL_NULLS_SORTED_HIGH = 0.
    b.append(I::SqlNullOrdering, 0i32);

    // Function lists. The values are kept in sync with the trino-compat
    // function registry (`sqe-trino-functions`) and the DataFusion stock
    // function set.
    b.append(
        I::SqlNumericFunctions,
        vec![
            "ABS".into(),
            "CEIL".into(),
            "FLOOR".into(),
            "ROUND".into(),
            "SQRT".into(),
            "POWER".into(),
            "MOD".into(),
            "EXP".into(),
            "LN".into(),
            "LOG10".into(),
            "SIGN".into(),
            "RAND".into(),
        ],
    );
    b.append(
        I::SqlStringFunctions,
        vec![
            "CONCAT".into(),
            "LENGTH".into(),
            "LOWER".into(),
            "UPPER".into(),
            "SUBSTRING".into(),
            "TRIM".into(),
            "LTRIM".into(),
            "RTRIM".into(),
            "REPLACE".into(),
            "POSITION".into(),
            "REGEXP_LIKE".into(),
            "REGEXP_REPLACE".into(),
        ],
    );
    b.append(
        I::SqlDatetimeFunctions,
        vec![
            "CURRENT_DATE".into(),
            "CURRENT_TIME".into(),
            "CURRENT_TIMESTAMP".into(),
            "DATE_TRUNC".into(),
            "DATE_DIFF".into(),
            "DATE_ADD".into(),
            "EXTRACT".into(),
            "FROM_UNIXTIME".into(),
            "TO_TIMESTAMP".into(),
            "YEAR".into(),
            "MONTH".into(),
            "DAY".into(),
        ],
    );
    b.append(
        I::SqlSystemFunctions,
        vec![
            "CURRENT_USER".into(),
            "CURRENT_CATALOG".into(),
            "CURRENT_SCHEMA".into(),
            "VERSION".into(),
        ],
    );

    // Reserved-word list (subset relevant to the engine's parser).
    b.append(
        I::SqlKeywords,
        vec![
            "ALTER".into(),
            "AND".into(),
            "AS".into(),
            "ASC".into(),
            "BETWEEN".into(),
            "BIGINT".into(),
            "BY".into(),
            "CASE".into(),
            "CAST".into(),
            "COLUMN".into(),
            "CREATE".into(),
            "DELETE".into(),
            "DESC".into(),
            "DISTINCT".into(),
            "DROP".into(),
            "ELSE".into(),
            "END".into(),
            "EXISTS".into(),
            "FALSE".into(),
            "FOR".into(),
            "FROM".into(),
            "FULL".into(),
            "GROUP".into(),
            "HAVING".into(),
            "IN".into(),
            "INNER".into(),
            "INSERT".into(),
            "INTO".into(),
            "IS".into(),
            "JOIN".into(),
            "LEFT".into(),
            "LIKE".into(),
            "LIMIT".into(),
            "MERGE".into(),
            "NOT".into(),
            "NULL".into(),
            "ON".into(),
            "OR".into(),
            "ORDER".into(),
            "OUTER".into(),
            "PARTITION".into(),
            "PREPARE".into(),
            "RIGHT".into(),
            "SELECT".into(),
            "SET".into(),
            "TABLE".into(),
            "THEN".into(),
            "TRUE".into(),
            "UNION".into(),
            "UPDATE".into(),
            "USING".into(),
            "VALUES".into(),
            "VIEW".into(),
            "WHEN".into(),
            "WHERE".into(),
            "WITH".into(),
        ],
    );
    b.append(I::SqlSearchStringEscape, "\\");
    b.append(I::SqlExtraNameCharacters, "");

    // Grammar levels are bitmasks over the enum ordinals: minimum | core
    // (SupportedSqlGrammar 0, 1) and ANSI-92 entry | intermediate
    // (SupportedAnsi92SqlGrammarLevel 0, 1).
    b.append(I::SqlSupportedGrammar, 0b011i32);
    b.append(I::SqlAnsi92SupportedLevel, 0b011i32);
    // GROUP BY unrelated | beyond select (bits 0, 1).
    b.append(I::SqlSupportedGroupBy, 0b11i32);
    // UNION | UNION ALL (bits 0, 1).
    b.append(I::SqlSupportedUnions, 0b11i32);
    // SqlOuterJoinsSupportLevel.SQL_FULL_OUTER_JOINS = 2.
    b.append(I::SqlOuterJoinsSupportLevel, 2i32);
    // Subqueries in comparisons | EXISTS | IN | quantified (bits 0-3).
    b.append(I::SqlSupportedSubqueries, 0b1111i32);

    // Common capability booleans.
    b.append(I::SqlSupportsColumnAliasing, true);
    b.append(I::SqlNullPlusNullIsNull, true);
    b.append(I::SqlSupportsTableCorrelationNames, true);
    b.append(I::SqlSupportsDifferentTableCorrelationNames, false);
    b.append(I::SqlSupportsExpressionsInOrderBy, true);
    b.append(I::SqlSupportsOrderByUnrelated, true);
    b.append(I::SqlSupportsLikeEscapeClause, true);
    b.append(I::SqlSupportsNonNullableColumns, true);
    b.append(I::SqlSupportsIntegrityEnhancementFacility, false);
    b.append(I::SqlSelectForUpdateSupported, false);
    b.append(I::SqlStoredProceduresSupported, false);
    b.append(I::SqlCorrelatedSubqueriesSupported, true);
    // SqlSupportedPositionedCommands.SQL_POSITIONED_DELETE = 0 / UPDATE = 1.
    // SQE doesn't support cursors; emit empty bitmask.
    b.append(I::SqlSupportedPositionedCommands, 0i32);

    // Limits. Iceberg + SQE have no fixed hard limits, but JDBC expects
    // a value. `0` means "unknown / no limit" per the spec.
    b.append(I::SqlMaxColumnNameLength, 1024i64);
    b.append(I::SqlMaxColumnsInGroupBy, 0i64);
    b.append(I::SqlMaxColumnsInIndex, 0i64);
    b.append(I::SqlMaxColumnsInOrderBy, 0i64);
    b.append(I::SqlMaxColumnsInSelect, 0i64);
    b.append(I::SqlMaxColumnsInTable, 0i64);
    b.append(I::SqlMaxConnections, 0i64);
    b.append(I::SqlMaxCursorNameLength, 0i64);
    b.append(I::SqlMaxIndexLength, 0i64);
    b.append(I::SqlDbSchemaNameLength, 1024i64);
    b.append(I::SqlMaxProcedureNameLength, 0i64);
    b.append(I::SqlMaxCatalogNameLength, 1024i64);
    b.append(I::SqlMaxRowSize, 0i64);
    b.append(I::SqlMaxRowSizeIncludesBlobs, true);
    b.append(I::SqlMaxStatementLength, 0i64);
    b.append(I::SqlMaxStatements, 0i64);
    b.append(I::SqlMaxTableNameLength, 1024i64);
    b.append(I::SqlMaxTablesInSelect, 0i64);
    b.append(I::SqlMaxUsernameLength, 1024i64);
    b.append(I::SqlMaxBinaryLiteralLength, 0i64);
    b.append(I::SqlMaxCharLiteralLength, 0i64);

    // Transactions (#821, #824). A transaction reads its own writes and the
    // latest committed state of every other table: READ_COMMITTED
    // (SqlTransactionIsolationLevel 2, bitmask 1 << 2). DDL inside a
    // client-held transaction runs autocommit and leaves the transaction
    // open: it neither commits the transaction nor is ignored.
    // Sent as int64: the Flight SQL JDBC driver casts this value to `Long`
    // and threw ClassCastException on an int32.
    b.append(I::SqlDefaultTransactionIsolation, 2i64);
    b.append(I::SqlTransactionsSupported, true);
    b.append(I::SqlSupportedTransactionsIsolationLevels, 1i32 << 2);
    b.append(I::SqlDataDefinitionCausesTransactionCommit, false);
    b.append(I::SqlDataDefinitionsInTransactionsIgnored, false);

    b.build()
        .map_err(|e| Status::internal(format!("Failed to build SQL info: {e}")))
}

/// SQL LIKE matching used for Flight SQL filter patterns.
///
/// `%` matches zero or more characters; `_` matches a single character.
/// All other characters match literally. Comparison is case-sensitive
/// because the Flight SQL spec keeps the identifier case the catalog
/// reports it as. Catalogs that fold to lower-case at the source still
/// match a lower-case pattern.
pub fn like_match(pattern: &str, value: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let v: Vec<char> = value.chars().collect();
    like_match_inner(&p, &v)
}

/// `"ns"."level"."table"` -- every dotted namespace level and the table name
/// double-quoted with embedded quotes doubled, so catalog-supplied names are
/// identifiers and never SQL (#712).
pub(crate) fn quote_qualified(ns: &str, table: &str) -> String {
    ns.split('.')
        .chain(std::iter::once(table))
        .map(|part| format!("\"{}\"", part.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(".")
}

fn like_match_inner(pattern: &[char], value: &[char]) -> bool {
    match (pattern.first(), value.first()) {
        (None, None) => true,
        (None, Some(_)) => false,
        (Some('%'), _) => {
            // % matches zero characters
            if like_match_inner(&pattern[1..], value) {
                return true;
            }
            // or any non-empty prefix
            if !value.is_empty() {
                return like_match_inner(pattern, &value[1..]);
            }
            false
        }
        (Some('_'), Some(_)) => like_match_inner(&pattern[1..], &value[1..]),
        (Some('_'), None) => false,
        (Some(p), Some(v)) if p == v => like_match_inner(&pattern[1..], &value[1..]),
        _ => false,
    }
}
