# Structured Error Handling — Flight SQL + Trino Standard Compliance

**Date:** 2026-03-30
**Status:** Draft
**Scope:** Replace stringly-typed errors with structured error codes that map correctly to both gRPC (Flight SQL) and Trino HTTP protocols, with proper logging and query ID propagation.

## Motivation

Current state:
- Flight SQL: all errors → `Status::internal()` (gRPC code 13), full internal error strings leaked
- Trino HTTP: all errors → `error_code: 1, INTERNAL_ERROR`, no category distinction
- Query ID not returned to clients — can't correlate errors with logs
- SQL text not logged at execution time
- `error_code` field in QueryTracker always `None`
- Type mismatch errors surface as `"Internal error: Expect TypeSignatureClass..."` instead of `"Type mismatch: lower() expects varchar, got boolean"`

After this change:
- Flight SQL returns correct gRPC status codes (INVALID_ARGUMENT, UNAUTHENTICATED, NOT_FOUND, INTERNAL)
- Trino HTTP returns correct Trino error codes matching real Trino behavior
- Query ID attached to every error response on both protocols
- User errors include safe detail; system errors are redacted
- Full error chain logged at WARN with query_id for debugging

## Error Code Taxonomy

```rust
/// Structured error codes for SQE. Each code maps to both gRPC and Trino
/// error representations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SqeErrorCode {
    // ── Parse / Planning (USER_ERROR in Trino, INVALID_ARGUMENT in gRPC) ──
    SyntaxError,          // Trino 1:  malformed SQL
    ParseError,           // Trino 1:  parser rejection
    SemanticError,        // Trino 2:  valid syntax, invalid semantics
    TypeMismatch,         // Trino 7:  wrong function arg types
    TableNotFound,        // Trino 11: table doesn't exist
    ColumnNotFound,       // Trino 12: column doesn't exist
    SchemaNotFound,       // Trino 55: namespace doesn't exist
    CatalogNotFound,      // Trino 44: catalog doesn't exist
    ViewNotFound,         // Trino 56: view doesn't exist
    FunctionNotFound,     // Trino 17: unknown function name
    InvalidArguments,     // Trino 4:  wrong number/type of args
    DuplicateTable,       // Trino 9:  table already exists
    DuplicateColumn,      // Trino 10: duplicate column
    DivisionByZero,       // Trino 3:  runtime division by zero
    InvalidCast,          // Trino 22: invalid type cast

    // ── Auth (USER_ERROR in Trino, UNAUTHENTICATED/PERMISSION_DENIED in gRPC) ──
    AuthenticationFailed, // Trino 6
    AccessDenied,         // Trino 6
    SessionExpired,       // Trino 6

    // ── Execution (INTERNAL_ERROR in Trino, INTERNAL in gRPC) ──
    ExecutionFailed,      // Trino 65536
    QueryTimeout,         // Trino 131078
    QueryCancelled,       // Trino 131079
    ResourceExhausted,    // Trino 131074

    // ── Catalog / Storage (EXTERNAL in Trino, INTERNAL in gRPC) ──
    CatalogError,         // Trino 65536
    StorageError,         // Trino 16777230
    CommitConflict,       // Trino 65536

    // ── Not supported ──
    NotSupported,         // Trino 14

    // ── Internal ──
    InternalError,        // Trino 65536
}
```

## Error Classification

Each error code has three properties:

```rust
impl SqeErrorCode {
    /// Is this a user error (safe to show details) or system error (redact)?
    pub fn is_user_error(&self) -> bool;

    /// gRPC status code for Flight SQL.
    pub fn grpc_code(&self) -> tonic::Code;

    /// Trino error representation.
    pub fn trino_error_code(&self) -> i32;
    pub fn trino_error_name(&self) -> &'static str;
    pub fn trino_error_type(&self) -> &'static str; // USER_ERROR | INTERNAL_ERROR | EXTERNAL
}
```

### Mapping tables

**gRPC codes:**

| SqeErrorCode | gRPC Code |
|---|---|
| SyntaxError..InvalidCast | INVALID_ARGUMENT (3) |
| TableNotFound, SchemaNotFound, CatalogNotFound, ViewNotFound | NOT_FOUND (5) |
| DuplicateTable | ALREADY_EXISTS (6) |
| AuthenticationFailed, SessionExpired | UNAUTHENTICATED (16) |
| AccessDenied | PERMISSION_DENIED (7) |
| NotSupported | UNIMPLEMENTED (12) |
| QueryTimeout | DEADLINE_EXCEEDED (4) |
| QueryCancelled | CANCELLED (1) |
| ResourceExhausted | RESOURCE_EXHAUSTED (8) |
| ExecutionFailed, CatalogError, StorageError, CommitConflict, InternalError | INTERNAL (13) |

**Trino error types:**

| SqeErrorCode | Trino type |
|---|---|
| SyntaxError..InvalidCast, Auth* | USER_ERROR |
| ExecutionFailed, QueryTimeout, QueryCancelled, ResourceExhausted, InternalError, CommitConflict | INTERNAL_ERROR |
| CatalogError, StorageError | EXTERNAL |
| NotSupported | USER_ERROR |

## Revised `SqeError`

```rust
pub struct SqeError {
    /// Structured error code.
    pub code: SqeErrorCode,
    /// User-safe message (always shown to client).
    pub message: String,
    /// Internal detail (only shown in debug mode, always logged).
    pub detail: Option<String>,
    /// Source error chain.
    pub source: Option<anyhow::Error>,
}
```

Builder pattern for ergonomic construction:

```rust
// User error — detail IS the message (safe to show)
SqeError::user(SqeErrorCode::TableNotFound, "Table 'test_ns.foo' not found")

// System error — message is generic, detail has internals
SqeError::internal(SqeErrorCode::StorageError, "Storage operation failed")
    .with_detail("S3 GetObject failed: connection refused to s3.amazonaws.com:443")

// From existing DataFusion errors — auto-classify
SqeError::from_datafusion(datafusion_error)  // parses error string to pick correct code
```

### Auto-classification from DataFusion errors

DataFusion errors have recognizable patterns that map to codes:

| DataFusion error pattern | SqeErrorCode |
|---|---|
| `"Error during planning: table '...' not found"` | TableNotFound |
| `"Error during planning: Invalid function"` | FunctionNotFound |
| `"Error during planning: Schema '...' not found"` | SchemaNotFound |
| `"Expect TypeSignatureClass..."` | TypeMismatch |
| `"No function matches..."` | InvalidArguments |
| Anything else planning | SemanticError |
| Anything else execution | ExecutionFailed |

## Protocol Integration

### Flight SQL

In `flight_sql.rs`, replace:

```rust
// Before:
.map_err(|e| Status::internal(format!("Query execution failed: {e}")))?;

// After:
.map_err(|e| e.to_grpc_status(&query_id))?;
```

`to_grpc_status` implementation:

```rust
impl SqeError {
    pub fn to_grpc_status(&self, query_id: &uuid::Uuid) -> tonic::Status {
        let code = self.code.grpc_code();
        let message = if self.code.is_user_error() {
            self.message.clone()
        } else {
            format!("{} [query_id={}]", self.code.default_message(), query_id)
        };
        let mut status = tonic::Status::new(code, message);
        // Attach query_id as metadata so clients can reference it
        status.metadata_mut().insert(
            "x-sqe-query-id",
            query_id.to_string().parse().unwrap(),
        );
        status
    }
}
```

### Trino HTTP

In `server.rs`, replace the hardcoded `TrinoError`:

```rust
// Before:
TrinoError {
    message: "Query execution failed".to_string(),
    error_code: 1,
    error_name: "INTERNAL_ERROR".to_string(),
    error_type: "INTERNAL_ERROR".to_string(),
}

// After:
e.to_trino_error(&query_id)
```

`to_trino_error` implementation:

```rust
impl SqeError {
    pub fn to_trino_error(&self, query_id: &uuid::Uuid) -> TrinoError {
        let message = if self.code.is_user_error() {
            self.message.clone()
        } else {
            format!("{} [query_id={}]", self.code.default_message(), query_id)
        };
        TrinoError {
            message,
            error_code: self.code.trino_error_code(),
            error_name: self.code.trino_error_name().to_string(),
            error_type: self.code.trino_error_type().to_string(),
            query_id: Some(query_id.to_string()),
        }
    }
}
```

Add `query_id` field to `TrinoError` (Trino protocol includes it):

```rust
pub struct TrinoError {
    pub message: String,
    pub error_code: i32,
    pub error_name: String,
    pub error_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_id: Option<String>,
}
```

## Query Lifecycle Logging

Every query gets structured logging at key points:

```
[INFO]  query.submit   query_id=abc sql="SELECT ..." user=alice source=dbt
[DEBUG] query.plan     query_id=abc plan_ms=5
[DEBUG] query.execute  query_id=abc rows=1000 batches=10
[INFO]  query.complete query_id=abc status=ok elapsed_ms=42 rows=1000
[WARN]  query.failed   query_id=abc status=error code=TABLE_NOT_FOUND
                        message="Table 'test_ns.foo' not found"
                        detail="DataFusion: table 'test_warehouse.test_ns.foo' not found"
                        elapsed_ms=3
```

Fields always present: `query_id`, `user`, `elapsed_ms`, `status`.
Fields on error: `code`, `message`, `detail`.
SQL text: logged at submit (INFO), not at execute (to avoid double-logging).

## Query Tracker Integration

Update `QueryTracker::failed()` to store the structured error:

```rust
pub fn failed(&self, query_id: &Uuid, error: &SqeError) {
    record.error_type = Some(error.code.trino_error_name().to_string());
    record.error_code = Some(error.code.trino_error_code().to_string());
    record.error_message = Some(error.message.clone());
    record.state = QueryState::Failed;
}
```

`system.runtime.queries` then shows useful error info instead of blanks.

## File Plan

| File | Action |
|---|---|
| `crates/sqe-core/src/error.rs` | Rewrite: `SqeErrorCode` enum, new `SqeError` struct, builder, protocol methods |
| `crates/sqe-core/src/lib.rs` | Update re-exports |
| `crates/sqe-coordinator/src/query_handler.rs` | Update error construction, add query lifecycle logging |
| `crates/sqe-coordinator/src/flight_sql.rs` | Use `to_grpc_status()`, attach query_id metadata |
| `crates/sqe-coordinator/src/query_tracker.rs` | Accept `SqeError` in `failed()`, store code+message |
| `crates/sqe-trino-compat/src/protocol.rs` | Add `query_id` to `TrinoError` |
| `crates/sqe-trino-compat/src/server.rs` | Use `to_trino_error()`, pass query_id |
| `crates/sqe-coordinator/src/write_handler.rs` | Update error construction |
| `crates/sqe-coordinator/src/catalog_ops.rs` | Update error construction |
| `crates/sqe-auth/src/provider.rs` | Map `AuthError` → `SqeError` with correct codes |

## Migration Strategy

The `SqeError` struct change is the biggest. To avoid a massive single PR:

1. Add `SqeErrorCode` enum + mapping methods (additive, no breaking changes)
2. Add `from_datafusion()` classifier
3. Update `SqeError` struct (breaking — all `?` sites need updating)
4. Update Flight SQL + Trino HTTP error responses
5. Update logging
6. Update QueryTracker

Steps 1-2 can be done without breaking anything. Step 3 is the big bang — every site that constructs or matches `SqeError` needs updating. But since this is an internal type (not public API), it's safe.
