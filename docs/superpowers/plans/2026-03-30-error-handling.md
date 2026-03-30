# Structured Error Handling Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace stringly-typed SqeError with structured error codes that map correctly to gRPC (Flight SQL) and Trino HTTP, with query ID propagation and proper logging.

**Architecture:** Add `SqeErrorCode` enum alongside the existing `SqeError` enum (non-breaking), then add protocol mapping methods, then update construction sites to use specific codes, then update Flight SQL + Trino HTTP to use the new mappings.

**Tech Stack:** Rust, tonic (gRPC), axum (Trino HTTP), tracing (logging)

**Spec:** `docs/superpowers/specs/2026-03-30-error-handling-design.md`

---

### Task 1: Add `SqeErrorCode` enum and protocol mapping methods

**Files:**
- Modify: `crates/sqe-core/src/error.rs`

This task is purely additive — the existing `SqeError` enum is unchanged.

- [ ] **Step 1: Add `SqeErrorCode` enum after the existing `SqeError` enum**

In `crates/sqe-core/src/error.rs`, add after line 65 (`pub type Result<T> = ...`):

```rust
/// Structured error codes for SQE. Each maps to both gRPC and Trino error representations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SqeErrorCode {
    // Parse / Planning
    SyntaxError,
    ParseError,
    SemanticError,
    TypeMismatch,
    TableNotFound,
    ColumnNotFound,
    SchemaNotFound,
    CatalogNotFound,
    ViewNotFound,
    FunctionNotFound,
    InvalidArguments,
    DuplicateTable,
    DuplicateColumn,
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
    // Catalog / Storage
    CatalogError,
    StorageError,
    CommitConflict,
    // Not supported
    NotSupported,
    // Internal
    InternalError,
}

impl SqeErrorCode {
    /// Whether this is a user error (safe to show details to client).
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

    /// The canonical name for this error code (used in logs and Trino error_name).
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
            SqeErrorCode::InvalidArguments => "INVALID_FUNCTION_ARGUMENT",
            SqeErrorCode::DuplicateTable => "TABLE_ALREADY_EXISTS",
            SqeErrorCode::DuplicateColumn => "DUPLICATE_COLUMN_NAME",
            SqeErrorCode::DivisionByZero => "DIVISION_BY_ZERO",
            SqeErrorCode::InvalidCast => "INVALID_CAST_ARGUMENT",
            SqeErrorCode::AuthenticationFailed => "PERMISSION_DENIED",
            SqeErrorCode::AccessDenied => "PERMISSION_DENIED",
            SqeErrorCode::SessionExpired => "PERMISSION_DENIED",
            SqeErrorCode::ExecutionFailed => "GENERIC_INTERNAL_ERROR",
            SqeErrorCode::QueryTimeout => "EXCEEDED_TIME_LIMIT",
            SqeErrorCode::QueryCancelled => "USER_CANCELED",
            SqeErrorCode::ResourceExhausted => "EXCEEDED_LOCAL_MEMORY_LIMIT",
            SqeErrorCode::CatalogError => "GENERIC_INTERNAL_ERROR",
            SqeErrorCode::StorageError => "GENERIC_EXTERNAL_ERROR",
            SqeErrorCode::CommitConflict => "GENERIC_INTERNAL_ERROR",
            SqeErrorCode::NotSupported => "NOT_SUPPORTED",
            SqeErrorCode::InternalError => "GENERIC_INTERNAL_ERROR",
        }
    }

    /// Trino error_code integer.
    pub fn trino_error_code(self) -> i32 {
        match self {
            SqeErrorCode::SyntaxError | SqeErrorCode::ParseError => 1,
            SqeErrorCode::SemanticError => 2,
            SqeErrorCode::DivisionByZero => 3,
            SqeErrorCode::InvalidArguments => 4,
            SqeErrorCode::AuthenticationFailed
            | SqeErrorCode::AccessDenied
            | SqeErrorCode::SessionExpired => 6,
            SqeErrorCode::TypeMismatch => 7,
            SqeErrorCode::DuplicateTable => 9,
            SqeErrorCode::DuplicateColumn => 10,
            SqeErrorCode::TableNotFound => 11,
            SqeErrorCode::ColumnNotFound => 12,
            SqeErrorCode::NotSupported => 14,
            SqeErrorCode::FunctionNotFound => 17,
            SqeErrorCode::InvalidCast => 22,
            SqeErrorCode::CatalogNotFound => 44,
            SqeErrorCode::SchemaNotFound => 55,
            SqeErrorCode::ViewNotFound => 56,
            SqeErrorCode::ExecutionFailed
            | SqeErrorCode::CatalogError
            | SqeErrorCode::CommitConflict
            | SqeErrorCode::InternalError => 65536,
            SqeErrorCode::ResourceExhausted => 131074,
            SqeErrorCode::QueryTimeout => 131078,
            SqeErrorCode::QueryCancelled => 131079,
            SqeErrorCode::StorageError => 16777230,
        }
    }

    /// Trino error_type string.
    pub fn trino_error_type(self) -> &'static str {
        match self {
            _ if self.is_user_error() => "USER_ERROR",
            SqeErrorCode::StorageError => "EXTERNAL",
            _ => "INTERNAL_ERROR",
        }
    }
}

impl std::fmt::Display for SqeErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}
```

- [ ] **Step 2: Add `error_code()` method to existing `SqeError`**

This bridges the old enum to the new code system. Add to the `impl SqeError` block:

```rust
    /// Infer a structured error code from this error.
    ///
    /// For the legacy `SqeError` variants this does best-effort classification
    /// based on the error message string. New code should construct errors with
    /// explicit codes via `SqeError::coded()`.
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
```

- [ ] **Step 3: Add classifier functions**

Add outside the impl block:

```rust
/// Classify a catalog error message into a specific error code.
fn classify_catalog_error(msg: &str) -> SqeErrorCode {
    let lower = msg.to_lowercase();
    if lower.contains("not found") || lower.contains("http 404") {
        if lower.contains("view") {
            SqeErrorCode::ViewNotFound
        } else if lower.contains("schema") || lower.contains("namespace") {
            SqeErrorCode::SchemaNotFound
        } else {
            SqeErrorCode::TableNotFound
        }
    } else if lower.contains("already exists") {
        SqeErrorCode::DuplicateTable
    } else {
        SqeErrorCode::CatalogError
    }
}

/// Classify an execution/planning error message into a specific error code.
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
    } else if lower.contains("syntax error") || lower.contains("expected") && lower.contains("found") {
        SqeErrorCode::SyntaxError
    } else if lower.contains("not yet supported") || lower.contains("not implemented") || lower.contains("not supported") {
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
```

- [ ] **Step 4: Update `client_message()` to use error code for user errors**

Replace the existing `client_message()` method:

```rust
    pub fn client_message(&self) -> String {
        let code = self.error_code();
        if code.is_user_error() {
            // User errors: show the actual message (it describes what the user did wrong)
            match self {
                SqeError::Auth(msg) => format!("Authentication failed: {msg}"),
                SqeError::NotImplemented(msg) => msg.clone(),
                SqeError::Execution(msg) | SqeError::Catalog(msg) => {
                    // Strip DataFusion wrapper noise for cleaner messages
                    clean_error_message(msg)
                }
                SqeError::Config(msg) => format!("Configuration error: {msg}"),
                SqeError::Internal(e) => format!("Internal error: {e}"),
            }
        } else {
            // System errors: generic message only (hide internals)
            match code {
                SqeErrorCode::CatalogError => "Catalog operation failed".to_string(),
                SqeErrorCode::StorageError => "Storage operation failed".to_string(),
                SqeErrorCode::ExecutionFailed => "Query execution failed".to_string(),
                SqeErrorCode::CommitConflict => "Commit conflict — please retry".to_string(),
                SqeErrorCode::InternalError => "Internal error".to_string(),
                _ => "Internal error".to_string(),
            }
        }
    }
```

Add the message cleaner:

```rust
/// Strip DataFusion wrapper prefixes for cleaner error messages.
fn clean_error_message(msg: &str) -> String {
    let msg = msg
        .trim_start_matches("SQL planning failed: ")
        .trim_start_matches("Error during planning: ")
        .trim_start_matches("Query execution failed: ")
        .trim_start_matches("Query execution error: ");
    // Clean up the TypeSignatureClass noise from DataFusion
    if msg.contains("TypeSignatureClass") {
        return "Type mismatch in function arguments".to_string();
    }
    msg.to_string()
}
```

- [ ] **Step 5: Add tests for error code classification**

Add to the `#[cfg(test)] mod tests` block:

```rust
    // -----------------------------------------------------------------------
    // Error code classification
    // -----------------------------------------------------------------------

    #[test]
    fn error_code_auth() {
        let err = SqeError::Auth("bad password".into());
        assert_eq!(err.error_code(), SqeErrorCode::AuthenticationFailed);
        assert!(err.error_code().is_user_error());
    }

    #[test]
    fn error_code_table_not_found_from_execution() {
        let err = SqeError::Execution(
            "SQL planning failed: Error during planning: table 'wh.ns.foo' not found".into(),
        );
        assert_eq!(err.error_code(), SqeErrorCode::TableNotFound);
    }

    #[test]
    fn error_code_function_not_found() {
        let err = SqeError::Execution("SQL planning failed: Invalid function 'year'".into());
        assert_eq!(err.error_code(), SqeErrorCode::FunctionNotFound);
    }

    #[test]
    fn error_code_type_mismatch() {
        let err = SqeError::Execution(
            "SQL planning failed: Internal error: Expect TypeSignatureClass::Native".into(),
        );
        assert_eq!(err.error_code(), SqeErrorCode::TypeMismatch);
    }

    #[test]
    fn error_code_not_supported() {
        let err = SqeError::NotImplemented("MERGE INTO is not yet supported".into());
        assert_eq!(err.error_code(), SqeErrorCode::NotSupported);
    }

    #[test]
    fn error_code_duplicate_table() {
        let err = SqeError::Catalog("Failed to create table: Table already exists".into());
        assert_eq!(err.error_code(), SqeErrorCode::DuplicateTable);
    }

    #[test]
    fn error_code_commit_conflict() {
        let err = SqeError::Execution(
            "Cannot add files that are already referenced by table".into(),
        );
        assert_eq!(err.error_code(), SqeErrorCode::CommitConflict);
    }

    #[test]
    fn error_code_trino_mapping() {
        assert_eq!(SqeErrorCode::SyntaxError.trino_error_code(), 1);
        assert_eq!(SqeErrorCode::SyntaxError.trino_error_type(), "USER_ERROR");
        assert_eq!(SqeErrorCode::TableNotFound.trino_error_code(), 11);
        assert_eq!(SqeErrorCode::InternalError.trino_error_code(), 65536);
        assert_eq!(SqeErrorCode::InternalError.trino_error_type(), "INTERNAL_ERROR");
        assert_eq!(SqeErrorCode::StorageError.trino_error_type(), "EXTERNAL");
    }

    #[test]
    fn client_message_shows_detail_for_user_errors() {
        let err = SqeError::Execution(
            "SQL planning failed: Error during planning: table 'test_ns.foo' not found".into(),
        );
        let msg = err.client_message();
        assert!(msg.contains("not found"), "got: {msg}");
        assert!(!msg.contains("SQL planning failed"), "should strip wrapper: {msg}");
    }

    #[test]
    fn client_message_hides_detail_for_system_errors() {
        let err = SqeError::Execution("S3 connection refused: s3.amazonaws.com".into());
        let msg = err.client_message();
        assert_eq!(msg, "Query execution failed");
        assert!(!msg.contains("s3.amazonaws.com"));
    }

    #[test]
    fn error_code_name_display() {
        assert_eq!(SqeErrorCode::TableNotFound.to_string(), "TABLE_NOT_FOUND");
        assert_eq!(SqeErrorCode::SyntaxError.name(), "SYNTAX_ERROR");
    }
```

- [ ] **Step 6: Re-export `SqeErrorCode` from lib.rs**

In `crates/sqe-core/src/lib.rs`, update:

```rust
pub use error::{Result, SqeError, SqeErrorCode};
```

- [ ] **Step 7: Run tests**

Run: `cargo test -p sqe-core`
Expected: All existing tests pass + new classification tests pass.

- [ ] **Step 8: Commit**

```bash
git add crates/sqe-core/src/error.rs crates/sqe-core/src/lib.rs
git commit -m "feat(core): add SqeErrorCode enum with gRPC and Trino mappings"
```

---

### Task 2: Update Flight SQL error responses

**Files:**
- Modify: `crates/sqe-coordinator/src/flight_sql.rs`

- [ ] **Step 1: Add `sqe_error_to_status` helper function**

At the top of `flight_sql.rs` (after imports), add:

```rust
/// Convert an SqeError to a gRPC Status with the correct status code
/// and query ID in metadata.
fn sqe_error_to_status(e: &sqe_core::SqeError, query_id: Option<&uuid::Uuid>) -> Status {
    let code = e.error_code();
    let grpc_code = match code {
        SqeErrorCode::SyntaxError | SqeErrorCode::ParseError | SqeErrorCode::SemanticError
        | SqeErrorCode::TypeMismatch | SqeErrorCode::InvalidArguments
        | SqeErrorCode::DivisionByZero | SqeErrorCode::InvalidCast
        | SqeErrorCode::ColumnNotFound | SqeErrorCode::DuplicateColumn => tonic::Code::InvalidArgument,
        SqeErrorCode::TableNotFound | SqeErrorCode::SchemaNotFound
        | SqeErrorCode::CatalogNotFound | SqeErrorCode::ViewNotFound
        | SqeErrorCode::FunctionNotFound => tonic::Code::NotFound,
        SqeErrorCode::DuplicateTable => tonic::Code::AlreadyExists,
        SqeErrorCode::AuthenticationFailed | SqeErrorCode::SessionExpired => tonic::Code::Unauthenticated,
        SqeErrorCode::AccessDenied => tonic::Code::PermissionDenied,
        SqeErrorCode::NotSupported => tonic::Code::Unimplemented,
        SqeErrorCode::QueryTimeout => tonic::Code::DeadlineExceeded,
        SqeErrorCode::QueryCancelled => tonic::Code::Cancelled,
        SqeErrorCode::ResourceExhausted => tonic::Code::ResourceExhausted,
        _ => tonic::Code::Internal,
    };

    let message = e.client_message();
    let mut status = Status::new(grpc_code, &message);

    // Attach error code and query ID as metadata for client diagnostics
    if let Ok(val) = code.name().parse() {
        status.metadata_mut().insert("x-sqe-error-code", val);
    }
    if let Some(qid) = query_id {
        if let Ok(val) = qid.to_string().parse() {
            status.metadata_mut().insert("x-sqe-query-id", val);
        }
    }

    // Log the full error server-side
    tracing::warn!(
        error_code = %code,
        query_id = ?query_id,
        client_message = %message,
        internal_detail = %e,
        "Query error"
    );

    status
}
```

- [ ] **Step 2: Replace all `Status::internal(format!("Query execution failed: {e}"))` calls**

Find every `.map_err(|e| Status::internal(...))` in `flight_sql.rs` that wraps an `SqeError` and replace with the new helper. The main site is the `do_get_statement` method (~line 352):

Replace:
```rust
.map_err(|e| Status::internal(format!("Query execution failed: {e}")))?;
```
With:
```rust
.map_err(|e| sqe_error_to_status(&e, None))?;
```

Do the same for all other `execute()` call sites in the file. Use `None` for query_id for now (Task 4 will thread it through).

- [ ] **Step 3: Add `SqeErrorCode` import**

Add to the imports at the top:
```rust
use sqe_core::SqeErrorCode;
```

- [ ] **Step 4: Build and test**

Run: `cargo build -p sqe-coordinator && cargo test -p sqe-coordinator`
Expected: All tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-coordinator/src/flight_sql.rs
git commit -m "feat(flight): use structured error codes in gRPC Status responses"
```

---

### Task 3: Update Trino HTTP error responses

**Files:**
- Modify: `crates/sqe-trino-compat/src/protocol.rs`
- Modify: `crates/sqe-trino-compat/src/server.rs`

- [ ] **Step 1: Add `query_id` field to `TrinoError`**

In `crates/sqe-trino-compat/src/protocol.rs`, update the struct:

```rust
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrinoError {
    pub message: String,
    pub error_code: i32,
    pub error_name: String,
    pub error_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_id: Option<String>,
}
```

- [ ] **Step 2: Add `from_sqe_error` constructor**

```rust
impl TrinoError {
    /// Create a TrinoError from an SqeError with proper error code mapping.
    pub fn from_sqe_error(e: &sqe_core::SqeError, query_id: Option<&str>) -> Self {
        let code = e.error_code();
        Self {
            message: e.client_message(),
            error_code: code.trino_error_code(),
            error_name: code.name().to_string(),
            error_type: code.trino_error_type().to_string(),
            query_id: query_id.map(|s| s.to_string()),
        }
    }
}
```

- [ ] **Step 3: Update `error_response` helper in `server.rs`**

Find the `error_response` function and the places where `TrinoError` is constructed with hardcoded `error_code: 1`. Replace with `TrinoError::from_sqe_error()` where an `SqeError` is available.

For the generic `error_response` helper (which takes a string, not SqeError), keep it but update it to accept an optional error code:

```rust
fn error_response(status: StatusCode, message: &str) -> Response {
    let error = TrinoError {
        message: message.to_string(),
        error_code: 1,
        error_name: "USER_ERROR".to_string(),
        error_type: "USER_ERROR".to_string(),
        query_id: None,
    };
    // ... existing response building
}
```

- [ ] **Step 4: Update `submit_query` error handling**

In the `submit_query` handler, where query execution errors are caught, replace the hardcoded TrinoError:

```rust
Err(e) => {
    let query_id = uuid::Uuid::new_v4().to_string();
    tracing::warn!(
        error_code = %e.error_code(),
        query_id = %query_id,
        error = %e,
        "Trino query execution failed"
    );
    let trino_error = TrinoError::from_sqe_error(&e, Some(&query_id));
    // ... build response with trino_error
}
```

- [ ] **Step 5: Build and test**

Run: `cargo build -p sqe-trino-compat && cargo test -p sqe-trino-compat`
Expected: All tests pass. Serialization tests may need updating for new `query_id` field.

- [ ] **Step 6: Commit**

```bash
git add crates/sqe-trino-compat/src/protocol.rs crates/sqe-trino-compat/src/server.rs
git commit -m "feat(trino): use structured error codes in Trino HTTP responses"
```

---

### Task 4: Query lifecycle logging + QueryTracker integration

**Files:**
- Modify: `crates/sqe-coordinator/src/query_handler.rs`
- Modify: `crates/sqe-coordinator/src/query_tracker.rs`

- [ ] **Step 1: Update `QueryRecord` to store error message**

In `query_tracker.rs`, add a field to `QueryRecord` after `error_code`:

```rust
    pub error_message: Option<String>,
```

- [ ] **Step 2: Update `QueryTracker::failed()` to accept `SqeError`**

Change the signature and implementation:

```rust
    pub fn failed(&self, query_id: &Uuid, error: &sqe_core::SqeError) {
        if let Some(old) = self.history.get(query_id) {
            let mut record = (*old).clone();
            let code = error.error_code();
            record.state = QueryState::Failed;
            record.ended = Some(Utc::now());
            record.error_type = Some(code.trino_error_type().to_string());
            record.error_code = Some(code.name().to_string());
            record.error_message = Some(error.client_message());
            self.history.insert(*query_id, Arc::new(record));
        }
        self.active.remove(query_id);
    }
```

Keep the old signature as a deprecated wrapper for any callers not yet migrated:

```rust
    #[deprecated(note = "use failed() with SqeError instead")]
    pub fn failed_legacy(&self, query_id: &Uuid, error_type: &str, error_code: Option<&str>) {
        if let Some(old) = self.history.get(query_id) {
            let mut record = (*old).clone();
            record.state = QueryState::Failed;
            record.ended = Some(Utc::now());
            record.error_type = Some(error_type.to_string());
            record.error_code = error_code.map(|s| s.to_string());
            self.history.insert(*query_id, Arc::new(record));
        }
        self.active.remove(query_id);
    }
```

- [ ] **Step 3: Update callers in `query_handler.rs`**

Find all `self.query_tracker.failed(...)` calls and update them to pass the `SqeError`:

The main error handling block (around line 280-295) should become:

```rust
            Err(e) => {
                let code = e.error_code();
                warn!(
                    query_id = %query_id,
                    error_code = %code,
                    user = %session.user.username,
                    message = %e.client_message(),
                    detail = %e,
                    elapsed_ms = start.elapsed().as_millis() as u64,
                    "Query failed"
                );
                self.query_tracker.failed(&query_id, &e);
                Err(e)
            }
```

- [ ] **Step 4: Add query_id to execute logging**

Update the initial execute log (around line 117) to include query_id:

```rust
        info!(
            query_id = %query_id,
            username = %session.user.username,
            sql_length = sql.len(),
            "Executing query"
        );
```

- [ ] **Step 5: Update `system.runtime.queries` to expose error_message**

Find where `QueryRecord` is mapped to the runtime table (around line 704-748) and add the `error_message` field to the output schema and data.

- [ ] **Step 6: Build and test**

Run: `cargo build --all && cargo test --all`
Expected: All tests pass. Some tests that call `query_tracker.failed()` will need updating.

- [ ] **Step 7: Commit**

```bash
git add crates/sqe-coordinator/src/query_handler.rs crates/sqe-coordinator/src/query_tracker.rs
git commit -m "feat: structured query lifecycle logging with error codes and query ID"
```

---

### Task 5: Full build + integration test sweep

- [ ] **Step 1: Build all**

Run: `cargo build --all`

- [ ] **Step 2: Run all unit tests**

Run: `cargo test --all`

- [ ] **Step 3: Clippy**

Run: `cargo clippy --all-targets -- -D warnings`

- [ ] **Step 4: Integration tests**

Run: `./scripts/integration-test.sh`

- [ ] **Step 5: Fix any issues**

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "chore: fix build/test issues from error handling refactor"
```
