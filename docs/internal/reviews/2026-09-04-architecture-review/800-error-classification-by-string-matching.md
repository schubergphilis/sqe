# 800: Client-facing error codes are derived by substring-matching error messages

- Scope: sqe-core (error.rs), every crate that constructs `SqeError`
- Severity: high
- Category: reliability
- Effort: L
- Status: open

## Problem

`SqeError` still carries most of its variants as bare strings, and the code
that turns an error into a Flight status or Trino error code sniffs the
lowercased message text.

`crates/sqe-core/src/error.rs:44-58`:

```rust
Auth(String),
Catalog(String),
Execution(String),
Config(String),
NotImplemented(String),
```

`crates/sqe-core/src/error.rs:286-288` routes the two most common variants
through keyword classifiers:

```rust
SqeError::Catalog(msg) => classify_catalog_error(msg),
SqeError::Execution(msg) => classify_execution_error(msg),
```

`classify_execution_error` (`error.rs:577-660`) is a chain of
`lower.contains("401 unauthorized")`, `contains("table") && contains("not found")`,
`contains("timeout")`, `contains("cancel")`, `contains("failed to allocate")`.
The same file at `error.rs:238` decides "is this a 404" with
`msg.contains("HTTP 404")`.

The structured alternative exists but is barely used: `SqeError::Sourced { code, .. }`
(`error.rs:91-97`) has 9 construction sites in the workspace, against 737
`SqeError::Catalog(format!(..))` / `SqeError::Execution(e.to_string())` style
constructions (count: `grep -rn -E 'SqeError::(Catalog|Execution|Auth|Config)\((format!|.*to_string\(\))' crates | grep -v /tests/`).
Sixty-one of those are literally `map_err(|e| SqeError::X(e.to_string()))`,
for example `crates/sqe-coordinator/src/hive_ddl_handler.rs:299,304,352,395,445,478`,
which drops the source chain (`std::error::Error::source()` returns `None`).

The classifier is load-bearing for behaviour, not only for display. The
comment at `error.rs:580-588` explains that the query handler's stale-bearer
auto-recovery only fires when the classifier returns `AuthenticationFailed`,
so whether a dbt run recovers from a token rotation depends on whether the
Polaris body happened to contain the string "401 unauthorized".

## Why it matters

- Any change to a message emitted by DataFusion, iceberg-rust, Polaris or
  reqwest silently changes the error code a client sees, and can silently
  disable retry/recovery logic. The two comment blocks at `error.rs:600-620`
  (DF 53 vs DF 54 wording) are the record of this already happening.
- The classifier reads user-controlled text. A table named `timeout_events`
  that is missing produces a message containing "table", "not found" and
  "timeout"; today the ordering of `if` branches decides the outcome.
- Log lines and audit records lose the causal chain, because the source error
  is flattened to a string at the first `map_err`.
- Every reviewer of every MR has to know the keyword list to know what an
  error will do downstream.

This is the June review item 4 (error type source chain loss) still open and
grown in surface.

## Proposed change

1. Make `SqeError::Sourced { code: SqeErrorCode, message, source: Option<Box<dyn Error + Send + Sync>> }`
   the primary variant. Keep the `String` variants as `#[deprecated]` shims
   for one release.
2. Add typed conversions at the boundaries where the code is actually known,
   instead of guessing it later:
   - `impl From<iceberg::Error>` mapping `ErrorKind` to `SqeErrorCode`.
   - `impl From<DataFusionError>` mapping `Plan`/`SchemaError`/`ResourcesExhausted`/`NotImplemented`
     to codes, falling back to `ExecutionFailed`.
   - `CatalogHttp { status, .. }` (already exists, `error.rs:62-70`) classified by
     status code only, never by body text.
3. Replace `map_err(|e| SqeError::Execution(e.to_string()))` with `?` once the
   `From` impls exist. Start with `sqe-coordinator/src/hive_ddl_handler.rs`,
   `write_handler.rs`, `query_handler.rs`.
4. Keep `classify_execution_error` only as a last-resort fallback for
   `Execution(String)` and log at `warn!` with a `sqe_error_classified_by_text_total`
   counter so remaining call sites can be found in production.
5. Move the auth auto-recovery trigger in `query_handler::execute` to check
   `error_code() == AuthenticationFailed` produced by the typed path.

## Acceptance criteria

- `grep -rn 'SqeError::Execution(e.to_string())' crates` returns zero hits outside tests.
- `SqeError::Sourced` is constructed in at least the catalog, write path and
  policy crates; `Execution(String)` construction count is below 100.
- New unit tests in `sqe-core/src/error.rs`: an `iceberg::Error` of kind
  `DataInvalid` maps to `TypeMismatch`; a `DataFusionError::ResourcesExhausted`
  maps to `ResourceExhausted`; a message containing both "table not found" and
  "timeout" is classified by its typed code, not its text.
- Trino parity run (`scripts/compare-*.sh`) shows unchanged error codes for
  the tempto allow-list.

## Related

- June 2026 codebase analysis, Critical item 4 and Improvement 5 (still open).
- 100-range files on sqe-core errors, if any.
- `crates/sqe-coordinator/src/flight_sql_helpers.rs:56` (`sqe_error_to_status`)
  and `crates/sqe-trino-compat/src/protocol.rs` consume `error_code()` and
  are the two client boundaries that benefit.
- Overlaps 104 (sqe-core view) and 305 (catalog view) and 709 (Trino view); this file is the cross-crate count and the auto-recovery dependence. Fix once, close all four.
