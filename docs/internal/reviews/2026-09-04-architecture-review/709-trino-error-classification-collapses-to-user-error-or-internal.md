# 709: Error classification on the Trino path collapses transport and auth failures to USER_ERROR and UDF argument errors to INTERNAL_ERROR

- Scope: sqe-trino-compat (server.rs, protocol.rs), sqe-trino-functions
- Severity: medium
- Category: operability
- Effort: S (hours)
- Status: open

## Problem

Three independent misclassifications combine on the Trino wire.

**1. Every HTTP-level failure is `USER_ERROR` code 1.** `error_response`, `crates/sqe-trino-compat/src/server.rs:512-541`, is used for 400, 401, 404 and 500 alike and hard-codes:

```rust
error: Some(TrinoError {
    message: msg.clone(),
    error_code: 1,
    error_name: "USER_ERROR".to_string(),
    error_type: "USER_ERROR".to_string(),
```

`TrinoError::user_error`, `protocol.rs:191-210`, names every user error `SYNTAX_ERROR` (code 1) even when the cause is "prepared statement not found" or "query task terminated unexpectedly" (`server.rs:1325-1328`, which is the panic path).

**2. UDF argument and type errors are `DataFusionError::Internal`.** `crates/sqe-trino-functions/src/trino_functions.rs` has 45 `DataFusionError::Internal` sites (grep count), for example:

- `:1274` `date_format: second arg must be a format string` (a user passing a column as the pattern)
- `:1316` `date_format: unsupported array type`
- `:2062` `Expected string array` (a `Utf8View` or `LargeUtf8` column)
- `helpers.rs:36-40` `Expected StringArray`

`SqeError::error_code()` maps `Internal` to an INTERNAL_ERROR class, so the client sees a server bug and operators page on a user mistake. Compare with `higher_order.rs`, which uses `exec_err!`/`plan_err!` (15 sites, 0 `Internal`) and is classified correctly.

**3. Rate limiting is detected by string match.** The `Retry-After` header is added when `trino_error.error_name == "RESOURCE_EXHAUSTED"` (`server.rs:1392`, `:1655`). The executor adapter produces that error as `SqeError::Execution(format!("rate limit: {e}"))` (`crates/sqe-coordinator/src/bin/sqe_server.rs:731-733`), which is not `RESOURCE_EXHAUSTED`, so the header is never set on the rate-limit path it was written for.

## Why it matters

Clients branch on `errorType`: dbt-trino retries `INTERNAL_ERROR` and `INSUFFICIENT_RESOURCES`, fails fast on `USER_ERROR`. Today a 401 is reported as a user SQL error (no retry with refreshed token), a rate-limit is reported without `Retry-After` (thundering-herd retry), and a mis-typed argument is reported as an engine crash. On the operator side, alerting on `INTERNAL_ERROR` rate is the standard SLO signal and it is polluted by user faults.

## Proposed change

1. Make `error_response` take a `SqeErrorCode` (or an explicit `TrinoErrorClass`) and map: 401 to `PERMISSION_DENIED`/`USER_ERROR` code 4, 404 to `NOT_FOUND`, 429 to `EXCEEDED_GLOBAL_MEMORY_LIMIT`-class `INSUFFICIENT_RESOURCES`, 500 to `GENERIC_INTERNAL_ERROR`. Give `user_error` an error-name parameter; the panic guard should emit `INTERNAL_ERROR`.
2. In `sqe-trino-functions`, replace `DataFusionError::Internal` for anything reachable from user input with `plan_err!` (type/arity at plan time) or `exec_err!` (runtime value errors). Reserve `Internal` for invariant violations. A clippy-style test: `grep -c 'DataFusionError::Internal' crates/sqe-trino-functions/src` must not exceed a small allow-listed number.
3. Return `SqeError::ResourceExhausted` (or the existing variant that maps to `RESOURCE_EXHAUSTED`) from `QueryHandlerAdapter::execute` on rate limit, so the existing header logic fires. Add a unit test for it.

## Acceptance criteria

- Unit tests assert `errorType`/`errorName` for: bad Basic credentials (401), unknown query id (404), rate-limited execute (429 with `Retry-After`), panic in executor (INTERNAL_ERROR).
- `SELECT date_format(ts, other_column) FROM t` returns `errorType: USER_ERROR` over Trino HTTP.
- `grep -c 'DataFusionError::Internal' crates/sqe-trino-functions/src/*.rs` total is under 10 and each remaining site has a comment naming the invariant.

## Related

- 703 (scalar-only UDFs; same files).
- 800-range cross-cutting error handling.
- `docs/internal/reviews/2026-07-25-production-readiness-review.md` item 24 credits the `SqeErrorCode` mapping; this issue is about the paths that bypass it.
