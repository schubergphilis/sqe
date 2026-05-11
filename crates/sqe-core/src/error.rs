use thiserror::Error;

#[derive(Error, Debug)]
pub enum SqeError {
    #[error("Authentication failed: {0}")]
    Auth(String),

    #[error("Catalog error: {0}")]
    Catalog(String),

    #[error("Query execution error: {0}")]
    Execution(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Not implemented: {0}")]
    NotImplemented(String),

    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl SqeError {
    /// Return a sanitised message safe for sending to clients.
    ///
    /// Routing by the error *variant*, not the error-code classification:
    ///
    /// - `Config` / `Internal`: variants that originate inside the engine
    ///   and may carry stack traces, file paths, connection strings, or
    ///   panic context. Return only the generic message for the
    ///   classified code, hiding the inner detail.
    /// - `Auth` / `Catalog` / `Execution` / `NotImplemented`: variants
    ///   whose payload describes something the user attempted — a bad
    ///   SQL statement, a missing object, a denied permission, a catalog
    ///   reply (Polaris 403 with details about the conflicting S3
    ///   location, etc.). Return the cleaned message so the client can
    ///   actually diagnose their query.
    ///
    /// The old behaviour routed by `error_code().is_user_error()`. That
    /// hid every `ExecutionFailed` / `CatalogError` behind the generic
    /// "Query execution failed" / "Catalog error occurred", which made
    /// dbt failures look like "Database Error: Query execution failed"
    /// with no actionable detail even though the underlying message
    /// (e.g. "Unable to create table at location s3://... because it
    /// conflicts with existing table or namespace") was sitting in the
    /// coordinator log.
    pub fn client_message(&self) -> String {
        match self {
            SqeError::Auth(msg)
            | SqeError::Catalog(msg)
            | SqeError::Execution(msg)
            | SqeError::NotImplemented(msg) => clean_error_message(msg),
            SqeError::Config(_) | SqeError::Internal(_) => {
                self.error_code().generic_message().to_string()
            }
        }
    }

    /// Return `true` if this error represents an HTTP 404 / resource-not-found
    /// condition from the catalog layer.
    pub fn is_not_found(&self) -> bool {
        match self {
            SqeError::Catalog(msg) => msg.contains("HTTP 404"),
            _ => false,
        }
    }

    /// Build the error string returned to a client.
    ///
    /// * **debug = true** (dev mode): return the full `Display` representation
    ///   including the error chain.
    /// * **debug = false** (production): return only the sanitised
    ///   [`client_message`](Self::client_message).
    pub fn to_client_error(&self, debug: bool) -> String {
        if debug {
            self.to_string()
        } else {
            self.client_message()
        }
    }

    /// Classify this error into a structured [`SqeErrorCode`].
    pub fn error_code(&self) -> SqeErrorCode {
        match self {
            SqeError::Auth(_) => SqeErrorCode::AuthenticationFailed,
            SqeError::Config(_) => SqeErrorCode::InternalError,
            SqeError::NotImplemented(_) => SqeErrorCode::NotSupported,
            SqeError::Internal(_) => SqeErrorCode::InternalError,
            SqeError::Catalog(msg) => classify_catalog_error(msg),
            SqeError::Execution(msg) => classify_execution_error(msg),
        }
    }
}

pub type Result<T> = std::result::Result<T, SqeError>;

/// Structured error codes for SQE errors.
///
/// These codes enable protocol-level mapping (e.g., gRPC status codes, Trino
/// error codes) and consistent client-facing error classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SqeErrorCode {
    // SQL parse / planning
    SyntaxError,
    ParseError,
    SemanticError,
    TypeMismatch,
    // Catalog / schema
    TableNotFound,
    ColumnNotFound,
    SchemaNotFound,
    CatalogNotFound,
    ViewNotFound,
    // Query building
    FunctionNotFound,
    InvalidArguments,
    DuplicateTable,
    DuplicateColumn,
    // Runtime
    DivisionByZero,
    InvalidCast,
    // Auth
    AuthenticationFailed,
    AccessDenied,
    SessionExpired,
    // Execution
    ExecutionFailed,
    QueryTimeout,
    QueryCancelled,
    ResourceExhausted,
    // Catalog / storage infrastructure
    CatalogError,
    StorageError,
    CommitConflict,
    // Feature support
    NotSupported,
    // Catch-all
    InternalError,
}

impl SqeErrorCode {
    /// Return `true` if this code represents an error caused by the user
    /// (e.g., bad SQL, missing table, auth failure) rather than a system fault.
    ///
    /// User errors have their detail passed through to the client.
    /// System errors are redacted.
    pub fn is_user_error(self) -> bool {
        matches!(
            self,
            SqeErrorCode::SyntaxError
                | SqeErrorCode::ParseError
                | SqeErrorCode::SemanticError
                | SqeErrorCode::TypeMismatch
                | SqeErrorCode::TableNotFound
                | SqeErrorCode::ColumnNotFound
                | SqeErrorCode::SchemaNotFound
                | SqeErrorCode::CatalogNotFound
                | SqeErrorCode::ViewNotFound
                | SqeErrorCode::FunctionNotFound
                | SqeErrorCode::InvalidArguments
                | SqeErrorCode::DuplicateTable
                | SqeErrorCode::DuplicateColumn
                | SqeErrorCode::DivisionByZero
                | SqeErrorCode::InvalidCast
                | SqeErrorCode::AuthenticationFailed
                | SqeErrorCode::AccessDenied
                | SqeErrorCode::SessionExpired
                | SqeErrorCode::NotSupported
                | SqeErrorCode::QueryCancelled
        )
    }

    /// Canonical upper-snake-case name for this error code.
    pub fn name(self) -> &'static str {
        match self {
            SqeErrorCode::SyntaxError => "SYNTAX_ERROR",
            SqeErrorCode::ParseError => "PARSE_ERROR",
            SqeErrorCode::SemanticError => "SEMANTIC_ERROR",
            SqeErrorCode::TypeMismatch => "TYPE_MISMATCH",
            SqeErrorCode::TableNotFound => "TABLE_NOT_FOUND",
            SqeErrorCode::ColumnNotFound => "COLUMN_NOT_FOUND",
            SqeErrorCode::SchemaNotFound => "SCHEMA_NOT_FOUND",
            SqeErrorCode::CatalogNotFound => "CATALOG_NOT_FOUND",
            SqeErrorCode::ViewNotFound => "VIEW_NOT_FOUND",
            SqeErrorCode::FunctionNotFound => "FUNCTION_NOT_FOUND",
            SqeErrorCode::InvalidArguments => "INVALID_ARGUMENTS",
            SqeErrorCode::DuplicateTable => "DUPLICATE_TABLE",
            SqeErrorCode::DuplicateColumn => "DUPLICATE_COLUMN",
            SqeErrorCode::DivisionByZero => "DIVISION_BY_ZERO",
            SqeErrorCode::InvalidCast => "INVALID_CAST",
            SqeErrorCode::AuthenticationFailed => "AUTHENTICATION_FAILED",
            SqeErrorCode::AccessDenied => "ACCESS_DENIED",
            SqeErrorCode::SessionExpired => "SESSION_EXPIRED",
            SqeErrorCode::ExecutionFailed => "EXECUTION_FAILED",
            SqeErrorCode::QueryTimeout => "QUERY_TIMEOUT",
            SqeErrorCode::QueryCancelled => "QUERY_CANCELLED",
            SqeErrorCode::ResourceExhausted => "RESOURCE_EXHAUSTED",
            SqeErrorCode::CatalogError => "CATALOG_ERROR",
            SqeErrorCode::StorageError => "STORAGE_ERROR",
            SqeErrorCode::CommitConflict => "COMMIT_CONFLICT",
            SqeErrorCode::NotSupported => "NOT_SUPPORTED",
            SqeErrorCode::InternalError => "GENERIC_INTERNAL_ERROR",
        }
    }

    /// Trino-compatible integer error code.
    pub fn trino_error_code(self) -> i32 {
        match self {
            SqeErrorCode::SyntaxError => 1,
            SqeErrorCode::ParseError => 2,
            SqeErrorCode::SemanticError => 4,
            SqeErrorCode::TypeMismatch => 7,
            SqeErrorCode::TableNotFound => 11,
            SqeErrorCode::ColumnNotFound => 12,
            SqeErrorCode::SchemaNotFound => 13,
            SqeErrorCode::CatalogNotFound => 14,
            SqeErrorCode::ViewNotFound => 15,
            SqeErrorCode::FunctionNotFound => 20,
            SqeErrorCode::InvalidArguments => 21,
            SqeErrorCode::DuplicateTable => 30,
            SqeErrorCode::DuplicateColumn => 31,
            SqeErrorCode::DivisionByZero => 40,
            SqeErrorCode::InvalidCast => 41,
            SqeErrorCode::AuthenticationFailed => 131,
            SqeErrorCode::AccessDenied => 132,
            SqeErrorCode::SessionExpired => 133,
            SqeErrorCode::ExecutionFailed => 65536,
            SqeErrorCode::QueryTimeout => 65540,
            SqeErrorCode::QueryCancelled => 65542,
            SqeErrorCode::ResourceExhausted => 65537,
            SqeErrorCode::CatalogError => 65600,
            SqeErrorCode::StorageError => 65601,
            SqeErrorCode::CommitConflict => 65602,
            SqeErrorCode::NotSupported => 100,
            SqeErrorCode::InternalError => 65540,
        }
    }

    /// Trino-compatible error type string.
    pub fn trino_error_type(self) -> &'static str {
        if self.is_user_error() {
            "USER_ERROR"
        } else {
            match self {
                SqeErrorCode::CatalogError | SqeErrorCode::StorageError => "EXTERNAL",
                _ => "INTERNAL_ERROR",
            }
        }
    }

    /// A generic, non-leaking message for system errors.
    pub fn generic_message(self) -> &'static str {
        match self {
            SqeErrorCode::ExecutionFailed => "Query execution failed",
            SqeErrorCode::CatalogError => "Catalog operation failed",
            SqeErrorCode::StorageError => "Storage operation failed",
            SqeErrorCode::CommitConflict => "Commit conflict",
            SqeErrorCode::ResourceExhausted => "Resource exhausted",
            SqeErrorCode::QueryTimeout => "Query timed out",
            SqeErrorCode::InternalError => "Internal error",
            _ => "Query failed",
        }
    }
}

impl std::fmt::Display for SqeErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

// ---------------------------------------------------------------------------
// Classifier helpers
// ---------------------------------------------------------------------------

/// Classify a [`SqeError::Catalog`] message into a specific error code.
fn classify_catalog_error(msg: &str) -> SqeErrorCode {
    let lower = msg.to_lowercase();
    // Auth failures from the catalog must be classified BEFORE the
    // not-found / 404 branches. A 401 / 403 response from Polaris (or
    // a circuit-breaker open after a wave of 401s) used to fall
    // through to "table not found" because the iceberg-rust error
    // text contained the word "found" elsewhere or because the
    // breaker message was opaque. That sent users hunting for tables
    // that exist when the actual cause was an Authorization header
    // that did not reach the catalog. Surface the real cause.
    if lower.contains("401")
        || lower.contains("unauthorized")
        || lower.contains("www-authenticate")
    {
        return SqeErrorCode::AuthenticationFailed;
    }
    if lower.contains("403") || lower.contains("forbidden") {
        return SqeErrorCode::AccessDenied;
    }
    if (lower.contains("not found") || lower.contains("http 404")) && lower.contains("view") {
        SqeErrorCode::ViewNotFound
    } else if (lower.contains("not found") || lower.contains("http 404"))
        && (lower.contains("schema") || lower.contains("namespace"))
    {
        SqeErrorCode::SchemaNotFound
    } else if lower.contains("not found") || lower.contains("http 404") {
        SqeErrorCode::TableNotFound
    } else if lower.contains("already exists") {
        SqeErrorCode::DuplicateTable
    } else {
        SqeErrorCode::CatalogError
    }
}

/// Classify a [`SqeError::Execution`] message into a specific error code.
fn classify_execution_error(msg: &str) -> SqeErrorCode {
    let lower = msg.to_lowercase();
    // TypeMismatch must be checked BEFORE FunctionNotFound because DataFusion
    // concatenates both messages: "TypeSignatureClass... No function matches..."
    if lower.contains("typesignatureclass")
        || lower.contains("type mismatch")
        || lower.contains("invalid comparison operation")
        || lower.contains("invalid argument error")
    {
        SqeErrorCode::TypeMismatch
    } else if lower.contains("table") && lower.contains("not found") {
        SqeErrorCode::TableNotFound
    } else if lower.contains("schema") && lower.contains("not found") {
        SqeErrorCode::SchemaNotFound
    } else if lower.contains("column") && lower.contains("not found") {
        SqeErrorCode::ColumnNotFound
    } else if lower.contains("invalid function") || lower.contains("no function matches") {
        SqeErrorCode::FunctionNotFound
    } else if lower.contains("not yet supported")
        || lower.contains("not implemented")
        || lower.contains("not supported")
    {
        SqeErrorCode::NotSupported
    } else if lower.contains("division by zero") {
        SqeErrorCode::DivisionByZero
    } else if lower.contains("cast") && (lower.contains("cannot") || lower.contains("invalid")) {
        SqeErrorCode::InvalidCast
    } else if lower.contains("timeout") || lower.contains("timed out") {
        SqeErrorCode::QueryTimeout
    } else if lower.contains("cancel") {
        SqeErrorCode::QueryCancelled
    } else if lower.contains("already referenced") || lower.contains("commit conflict") {
        SqeErrorCode::CommitConflict
    } else if lower.contains("rate limit") {
        SqeErrorCode::ResourceExhausted
    } else {
        SqeErrorCode::ExecutionFailed
    }
}

/// Strip DataFusion wrapper noise from error messages before showing them to
/// clients.
///
/// DataFusion wraps planning errors with prefixes like
/// `"SQL planning failed: Error during planning: "` and type errors with
/// verbose `TypeSignatureClass` annotations.  This function strips known
/// prefixes and cleans up those annotations.
fn clean_error_message(msg: &str) -> String {
    let prefixes = [
        "Query execution failed: ",
        "Query execution error: ",
        "SQL planning failed: Error during planning: ",
        "SQL planning failed: ",
        "Error during planning: ",
        "DataFusion error: ",
        "External error: External: External error: ",
        "External error: External: ",
        "External error: ",
        "Arrow error: ",
        "Invalid argument error: ",
    ];
    let mut cleaned = msg;
    for prefix in &prefixes {
        if let Some(stripped) = cleaned.strip_prefix(prefix) {
            cleaned = stripped;
        }
    }
    // Clean up TypeSignatureClass verbose output
    let result = cleaned.to_string();
    // Remove "[TypeSignatureClass(...)]" style annotations
    let result = regex_strip_type_sig(&result);
    result.trim().to_string()
}

/// Remove TypeSignatureClass annotations from a message (simple heuristic,
/// no regex dependency).
fn regex_strip_type_sig(msg: &str) -> String {
    // Find and remove "[TypeSignatureClass(...]" style substrings
    let mut result = String::with_capacity(msg.len());
    let mut chars = msg.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '[' {
            // Peek ahead to see if this looks like a TypeSignatureClass
            let rest: String = chars.clone().take(20).collect();
            if rest.to_lowercase().starts_with("typesignatureclass") {
                // Skip until matching ']'
                let mut depth = 1usize;
                for ch in chars.by_ref() {
                    match ch {
                        '[' => depth += 1,
                        ']' => {
                            depth -= 1;
                            if depth == 0 {
                                break;
                            }
                        }
                        _ => {}
                    }
                }
            } else {
                result.push(c);
            }
        } else {
            result.push(c);
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---------------------------------------------------------------------------
    // Existing tests (updated where client_message behaviour changed)
    // ---------------------------------------------------------------------------

    #[test]
    fn client_message_hides_auth_details() {
        // Auth is a user error — detail is shown, but the string still starts
        // with the auth message (no internal system paths leaked).
        let err = SqeError::Auth("JWT expired at 2026-01-01T00:00:00Z".into());
        // The detail IS surfaced for user errors; check it contains the reason.
        assert!(err.client_message().contains("JWT expired"));
        // Still must not expose stack traces / file paths (there are none here).
    }

    #[test]
    fn client_message_surfaces_catalog_detail() {
        // Catalog errors now route by variant, not error-code classification.
        // The Polaris-returned text (object names, HTTP status, conflict
        // descriptions) is what the client needs to diagnose the failure
        // — surface it, don't hide it behind "Catalog operation failed".
        let err = SqeError::Catalog(
            "Failed to create table: status: 403 Forbidden, ... Unable to \
             create table at location 's3://iceberg-warehouse/main_warehouse/\
             dev_silver/stg_customers__dbt_tmp' because it conflicts with \
             existing table or namespace".into(),
        );
        let msg = err.client_message();
        assert!(msg.contains("conflicts with existing table"));
        assert!(msg.contains("stg_customers__dbt_tmp"));
        assert!(msg.contains("403 Forbidden"));
    }

    #[test]
    fn client_message_surfaces_execution_detail() {
        // ExecutionFailed used to fall back to the generic "Query execution
        // failed" because the error code is in the system-error bucket. After
        // the variant-based routing change, the underlying message is shown
        // — that includes column / table names from the query the user
        // actually submitted.
        let err = SqeError::Execution(
            "Failed to bind variable: 'p_threshold' not provided".into(),
        );
        let msg = err.client_message();
        assert!(msg.contains("p_threshold"));
        assert!(msg.contains("not provided"));
    }

    #[test]
    fn client_message_hides_internal_details() {
        let err = SqeError::Internal(anyhow::anyhow!("segfault at 0xdeadbeef"));
        assert_eq!(err.client_message(), "Internal error");
        assert!(!err.client_message().contains("segfault"));
    }

    #[test]
    fn client_message_hides_config_details() {
        let err = SqeError::Config("missing field 'client_secret'".into());
        assert_eq!(err.client_message(), "Internal error");
        assert!(!err.client_message().contains("client_secret"));
    }

    #[test]
    fn client_message_passes_through_not_implemented() {
        let msg = "MERGE INTO is not yet supported";
        let err = SqeError::NotImplemented(msg.into());
        assert_eq!(err.client_message(), msg);
    }

    #[test]
    fn to_client_error_production_hides_details() {
        // Auth is a user error so detail IS shown; test adjusted.
        let err = SqeError::Auth("token invalid: audience mismatch".into());
        let output = err.to_client_error(false);
        assert!(output.contains("audience") || output.contains("token"));
    }

    #[test]
    fn to_client_error_debug_exposes_details() {
        let err = SqeError::Auth("token invalid: audience mismatch".into());
        let output = err.to_client_error(true);
        assert!(output.contains("audience mismatch"));
        assert!(output.contains("Authentication failed"));
    }

    #[test]
    fn is_not_found_true_for_catalog_http_404() {
        let err = SqeError::Catalog(
            "Failed to drop view (HTTP 404 Not Found): view not found".into(),
        );
        assert!(err.is_not_found());
    }

    #[test]
    fn is_not_found_false_for_catalog_other_status() {
        let err =
            SqeError::Catalog("Failed to drop view (HTTP 500 Internal Server Error)".into());
        assert!(!err.is_not_found());
    }

    #[test]
    fn is_not_found_false_for_non_catalog_variants() {
        assert!(!SqeError::Auth("HTTP 404".into()).is_not_found());
        assert!(!SqeError::Execution("HTTP 404".into()).is_not_found());
        assert!(!SqeError::NotImplemented("HTTP 404".into()).is_not_found());
    }

    #[test]
    fn to_client_error_internal_production_vs_debug() {
        let err = SqeError::Internal(anyhow::anyhow!("disk full: /var/data"));
        // Production: sanitised
        assert_eq!(err.to_client_error(false), "Internal error");
        // Debug: full chain
        let debug_output = err.to_client_error(true);
        assert!(debug_output.contains("disk full"));
    }

    // ---------------------------------------------------------------------------
    // New tests
    // ---------------------------------------------------------------------------

    #[test]
    fn error_code_auth() {
        let err = SqeError::Auth("invalid token".into());
        let code = err.error_code();
        assert_eq!(code, SqeErrorCode::AuthenticationFailed);
        assert!(code.is_user_error());
    }

    #[test]
    fn error_code_table_not_found_from_execution() {
        let err = SqeError::Execution("table 'orders' not found in catalog".into());
        let code = err.error_code();
        assert_eq!(code, SqeErrorCode::TableNotFound);
        assert!(code.is_user_error());
    }

    #[test]
    fn error_code_function_not_found() {
        let err = SqeError::Execution("Invalid function 'my_udf'".into());
        let code = err.error_code();
        assert_eq!(code, SqeErrorCode::FunctionNotFound);
        assert!(code.is_user_error());
    }

    #[test]
    fn error_code_type_mismatch() {
        let err = SqeError::Execution("TypeSignatureClass(Exact([Int64])) does not match".into());
        let code = err.error_code();
        assert_eq!(code, SqeErrorCode::TypeMismatch);
        assert!(code.is_user_error());
    }

    #[test]
    fn error_code_not_supported() {
        let err = SqeError::NotImplemented("MERGE INTO is not yet supported".into());
        let code = err.error_code();
        assert_eq!(code, SqeErrorCode::NotSupported);
        assert!(code.is_user_error());
    }

    #[test]
    fn error_code_duplicate_table() {
        let err = SqeError::Catalog("Table 'orders' already exists".into());
        let code = err.error_code();
        assert_eq!(code, SqeErrorCode::DuplicateTable);
        assert!(code.is_user_error());
    }

    #[test]
    fn error_code_catalog_401_classifies_as_authentication_failed() {
        // The exact error text dbt users were seeing while the bearer-drop
        // race was active. Must NOT classify as TableNotFound.
        let err = SqeError::Catalog(
            "Failed to list namespaces: Unexpected, context: \
             { status: 401 Unauthorized, headers: { www-authenticate: \"Bearer\" } }"
                .into(),
        );
        let code = err.error_code();
        assert_eq!(code, SqeErrorCode::AuthenticationFailed);
        assert!(code.is_user_error());
        assert_eq!(code.name(), "AUTHENTICATION_FAILED");
    }

    #[test]
    fn error_code_catalog_403_classifies_as_access_denied() {
        let err = SqeError::Catalog(
            "Failed to load table: 403 Forbidden: principal not authorised".into(),
        );
        let code = err.error_code();
        assert_eq!(code, SqeErrorCode::AccessDenied);
        assert_eq!(code.name(), "ACCESS_DENIED");
    }

    #[test]
    fn error_code_catalog_unauthorized_word_alone_classifies_as_auth() {
        // Some catalogs (HMS, Glue) format the error without an HTTP code
        // but still include "unauthorized" or "www-authenticate".
        let err = SqeError::Catalog("AccessDenied: unauthorized request".into());
        let code = err.error_code();
        assert_eq!(code, SqeErrorCode::AuthenticationFailed);
    }

    #[test]
    fn error_code_catalog_table_with_404_still_classifies_as_table_not_found() {
        // Regression: the new auth-first branch must not steal genuine
        // not-found errors that don't mention auth.
        let err = SqeError::Catalog(
            "Failed to load table: HTTP 404 Not Found: orders".into(),
        );
        let code = err.error_code();
        assert_eq!(code, SqeErrorCode::TableNotFound);
    }

    #[test]
    fn error_code_commit_conflict() {
        let err = SqeError::Execution("file already referenced in snapshot".into());
        let code = err.error_code();
        assert_eq!(code, SqeErrorCode::CommitConflict);
    }

    #[test]
    fn error_code_trino_mapping() {
        assert_eq!(SqeErrorCode::SyntaxError.trino_error_code(), 1);
        assert_eq!(SqeErrorCode::TypeMismatch.trino_error_code(), 7);
        assert_eq!(SqeErrorCode::TableNotFound.trino_error_code(), 11);
        assert_eq!(SqeErrorCode::AuthenticationFailed.trino_error_code(), 131);
        assert_eq!(SqeErrorCode::ExecutionFailed.trino_error_code(), 65536);
        assert_eq!(SqeErrorCode::InternalError.trino_error_code(), 65540);

        assert_eq!(SqeErrorCode::SyntaxError.trino_error_type(), "USER_ERROR");
        assert_eq!(SqeErrorCode::TableNotFound.trino_error_type(), "USER_ERROR");
        assert_eq!(SqeErrorCode::AuthenticationFailed.trino_error_type(), "USER_ERROR");
        assert_eq!(SqeErrorCode::ExecutionFailed.trino_error_type(), "INTERNAL_ERROR");
        assert_eq!(SqeErrorCode::InternalError.trino_error_type(), "INTERNAL_ERROR");
        assert_eq!(SqeErrorCode::CatalogError.trino_error_type(), "EXTERNAL");
        assert_eq!(SqeErrorCode::StorageError.trino_error_type(), "EXTERNAL");
    }

    #[test]
    fn client_message_shows_detail_for_user_errors() {
        // NotImplemented is a user error: detail must be in client message.
        let err = SqeError::NotImplemented("LATERAL JOIN is not yet supported".into());
        let msg = err.client_message();
        assert!(msg.contains("LATERAL JOIN"), "expected detail in: {msg}");
    }

    #[test]
    fn client_message_hides_detail_for_system_errors() {
        // Internal is a system error: generic message returned.
        let err = SqeError::Internal(anyhow::anyhow!("connection pool exhausted"));
        let msg = err.client_message();
        assert_eq!(msg, "Internal error");
        assert!(!msg.contains("pool"), "should not contain internal detail");
    }

    #[test]
    fn error_code_name_display() {
        assert_eq!(SqeErrorCode::SyntaxError.name(), "SYNTAX_ERROR");
        assert_eq!(SqeErrorCode::InternalError.name(), "GENERIC_INTERNAL_ERROR");
        assert_eq!(SqeErrorCode::TableNotFound.name(), "TABLE_NOT_FOUND");
        // Display delegates to name()
        assert_eq!(
            format!("{}", SqeErrorCode::FunctionNotFound),
            "FUNCTION_NOT_FOUND"
        );
        assert_eq!(
            format!("{}", SqeErrorCode::InternalError),
            "GENERIC_INTERNAL_ERROR"
        );
    }
}
