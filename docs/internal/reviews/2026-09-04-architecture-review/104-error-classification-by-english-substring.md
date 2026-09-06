# 104: Error codes are derived by substring-matching English messages; 739 call sites still build the untyped variants

- Scope: sqe-core (error), all crates as producers
- Severity: medium
- Category: reliability
- Effort: L (week+)
- Status: open
- Issue: #547

## Problem

`SqeError::error_code` (`crates/sqe-core/src/error.rs:270-293`) dispatches the
two most common variants to text classifiers:

```rust
SqeError::Catalog(msg) => classify_catalog_error(msg),
SqeError::Execution(msg) => classify_execution_error(msg),
```

`classify_execution_error` (`error.rs:577-660`) is a chain of
`lower.contains(...)` tests: `"typesignatureclass"`, `"add explicit type
casts"`, `"table"` and `"not found"`, `"cancel"`, `"already referenced"`,
`"failed to allocate"`, `"timeout"`. `classify_catalog_error`
(`error.rs:507-575`) does the same for `"401"`, `"www-authenticate"`,
`"circuit breaker"`, `"bad gateway"`, `"dns"`. `is_not_found`
(`error.rs:236-249`) checks `msg.contains("HTTP 404")`.

The structured alternative exists (`SqeError::Sourced`, `CatalogHttp`,
`ExecutionAuth`, `*_src` constructors, added for #268) but adoption is
marginal. Counting non-test constructions across the workspace:

| Constructor | Sites |
|---|---|
| `SqeError::Execution(String)` | 739 |
| `SqeError::Catalog(String)` | 80 |
| `SqeError::Auth(String)` | 34 |
| `SqeError::Config(String)` | 38 |
| `catalog_src(...)` | 18 |
| `execution_src(...)` | 10 |
| `SqeError::sourced(...)` | 6 |
| `CatalogHttp { .. }` | 3 |
| `auth_src(...)` | 0 |

So over 95 percent of errors are strings, and their code, Trino error type,
retryability and "is this the user's fault" are all decided by whether a
DataFusion, iceberg-rust, reqwest or object_store message happens to contain
one of those words.

## Why it matters

Three consumers depend on `error_code()` being right:

- `SqeErrorCode::is_user_error` (`error.rs:353`) decides whether the message
  is shown to the client or replaced by a generic string.
- `trino_error_code` / `trino_error_type` (`error.rs:415-463`) are what dbt
  and BI tools branch on for retries.
- `is_not_found` drives create-if-not-exists and view resolution.

Each DataFusion or iceberg-rust upgrade rewords some messages. The DF55/arrow59
upgrade is in progress. When "Query execution error: ... timed out" becomes
"deadline exceeded", the code silently becomes `ExecutionFailed`, dbt stops
retrying, and no test fails because nothing asserts the mapping for upstream
text. A message that merely mentions a table (`"filter on table x cancelled"`)
is classified by whichever `contains` wins first.

## Proposed change

1. Make the typed path the default: change `SqeError::Execution(String)` to
   require a code, or add `SqeError::execution(code, msg)` and mark the
   string variant `#[deprecated]` so clippy flags new uses.
2. Map upstream errors once at the boundary, by variant not by text:
   `DataFusionError::ResourcesExhausted` to `ResourceExhausted`,
   `DataFusionError::Plan`/`SchemaError` to `SemanticError`,
   `iceberg::ErrorKind::*` and the REST status code to `CatalogHttp`. A single
   `From<DataFusionError> for SqeError` in `sqe-core` (or a helper crate that
   is allowed to depend on datafusion) replaces most of the 739 sites with `?`.
3. Keep `classify_*_error` only as a last-resort fallback for the deprecated
   variants, and add a tracing WARN counter when it fires so remaining sites
   can be found in production.
4. Migrate crate by crate; `sqe-coordinator/src/write_handler.rs` and
   `query_handler.rs` hold most of the 739.

## Acceptance criteria

- `grep -rn 'SqeError::Execution(' crates --include=*.rs | grep -v test | wc -l`
  drops below 100, with the remainder annotated.
- Unit tests in `sqe-core` assert code mapping from constructed
  `DataFusionError` variants, not from strings.
- A metric or log counter `sqe_error_classified_by_text_total` exists and is
  zero in `make test-integration`.

## Related

- Codebase analysis 2026-06-26 item 4 (string error variants) partially
  addressed by #268; still holds, reported here as a new file.
- Scope 800 cross-cutting error-handling review may pick up the client-message
  leakage side; this file is about classification.
- Overlaps `305-error-classification-by-substring.md` (catalog side) and `800-error-classification-by-string-matching.md` (cross-cutting); this file owns the `sqe-core` type and the migration counts.
