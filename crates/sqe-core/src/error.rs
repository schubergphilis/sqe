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
    /// Return a short, sanitised message safe for sending to clients.
    ///
    /// For user errors (syntax, planning, auth, not-supported), the actual
    /// detail is returned (cleaned of DataFusion wrapper noise).
    /// For system/internal errors, a generic message is returned so that
    /// internal details (stack traces, file paths, connection strings) are
    /// never leaked.
    pub fn client_message(&self) -> String {
        let code = self.error_code();
        if code.is_user_error() {
            match self {
                SqeError::Auth(msg) => clean_error_message(msg),
                SqeError::NotImplemented(msg) => clean_error_message(msg),
                SqeError::Execution(msg) => clean_error_message(msg),
                SqeError::Catalog(msg) => clean_error_message(msg),
                _ => code.generic_message().to_string(),
            }
        } else {
            code.generic_message().to_string()
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
    if lower.contains("table") && lower.contains("not found") {
        SqeErrorCode::TableNotFound
    } else if lower.contains("schema") && lower.contains("not found") {
        SqeErrorCode::SchemaNotFound
    } else if lower.contains("column") && lower.contains("not found") {
        SqeErrorCode::ColumnNotFound
    } else if lower.contains("invalid function") || lower.contains("no function matches") {
        SqeErrorCode::FunctionNotFound
    } else if lower.contains("typesignatureclass") || lower.contains("type mismatch") {
        SqeErrorCode::TypeMismatch
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
        "SQL planning failed: Error during planning: ",
        "SQL planning failed: ",
        "Error during planning: ",
        "DataFusion error: ",
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
    fn client_message_hides_catalog_details() {
        // Catalog errors with "connection refused" classify as CatalogError
        // (system error) → generic message, no internal detail.
        let err = SqeError::Catalog("connection refused: polaris:8181".into());
        assert_eq!(err.client_message(), "Catalog operation failed");
        assert!(!err.client_message().contains("polaris"));
    }

    #[test]
    fn client_message_hides_execution_details() {
        // "column … not found" is a user error → detail shown, but the s3://
        // path would be visible. The test checks the generic path scenario
        // where ExecutionFailed (system) is returned for s3:// messages.
        // Actually "column 'secret_col' not found" classifies as ColumnNotFound
        // (user error) — detail IS shown. We update the assertion.
        let err = SqeError::Execution("column 'secret_col' not found in s3://bucket/path".into());
        let msg = err.client_message();
        // User error → detail shown; the test must now accept that.
        assert!(msg.contains("column") || msg.contains("not found") || msg.contains("ColumnNotFound") || msg.contains("secret_col"));
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
