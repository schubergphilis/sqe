# 708: Trino session-control and prepared-statement handling is prefix-string parsed, bypasses the SQL parser, and echoes state changes before the executor confirms them

- Scope: sqe-trino-compat (protocol.rs, prepared.rs, server.rs), sqe-core (sql_params.rs)
- Severity: medium
- Category: reliability
- Effort: M (days)
- Status: open
- Issue: #628

## Problem

`parse_session_statement`, `crates/sqe-trino-compat/src/protocol.rs:419-490`, recognizes `USE`, `SET CATALOG`, `SET SESSION`, `RESET SESSION`, `PREPARE`, and `DEALLOCATE PREPARE` by uppercased prefix and hand-split:

```rust
if let Some(rest) = strip_prefix_ci(trimmed, &upper, "SET SESSION ") {
    if let Some((name, value)) = rest.split_once('=') { ... }
}
...
fn split_prepare_body(s: &str) -> Option<(&str, &str)> {
    let upper = s.to_uppercase();
    let from = upper.find(" FROM ")?;
```

`PREPARE`, `DEALLOCATE`, `SET SESSION`, and `RESET SESSION` never reach the executor (`server.rs:1140-1180`). Consequences visible in the code:

- `PREPARE p FROM SELEC 1` is accepted; the syntax error surfaces only at `EXECUTE`. Trino validates at PREPARE time, and JDBC drivers rely on that for `prepareStatement` failure.
- `SET SESSION anything = whatever` is accepted and echoed (`server.rs:1150-1155` comment "accept-and-echo"). A client that sets `query_max_memory` believes it took effect.
- `USE` is passed to the executor, but on the "still running" arm the session headers are applied before the executor has finished (`server.rs:1410-1418`), so a `USE missing_schema` that fails after the one-second bounded wait has already switched the client's replayed `X-Trino-Schema`.
- `EXECUTE` resolution, `crates/sqe-trino-compat/src/prepared.rs:154-200`, splits on whitespace and re-implements a string-literal scanner (`parse_string_literal`, `:96-117`; `split_top_level`, `:69-93`). `USING` arguments are spliced verbatim into the template by `sqe_core::substitute_placeholders`, `crates/sqe-core/src/sql_params.rs:15-55`, which tracks single and double quotes but not `--` or `/* */` comments, `E'...'` strings, or dollar-quoted strings. A `?` inside a comment consumes a parameter and the bind count check then fails with a misleading count, or, with a matching count, binds into the wrong position.
- `parse_show_session` (`protocol.rs:492-520`) is also a prefix matcher and returns "all properties" for any unrecognized tail rather than an error.

All of this sits beside a real parser: `sqe-sql` already extends sqlparser with custom statements (`GRANT ... MASKED WITH`, `ATTACH`, `CREATE SECRET`). The Trino session grammar is smaller than those.

## Why it matters

Trino clients (JDBC, dbt-trino, Metabase) treat the response to `PREPARE` and `USE` as authoritative. Accepting invalid state and echoing it back means client-side state drifts from server-side reality with no error at the point of the mistake. Prefix matching also fails on leading comments (`-- ...\nUSE x` is not recognized because `trim_start` does not strip comments) and passes them to the executor instead.

## Proposed change

1. Add `sqe_sql::StatementKind::{Use, SetCatalog, SetSession, ResetSession, Prepare, Deallocate, Execute, DescribeOutput, DescribeInput, ShowSession}` produced by the existing custom-statement parser. sqlparser already parses `PREPARE`/`EXECUTE`/`DEALLOCATE`/`USE`; `SET SESSION`, `SHOW SESSION` and `DESCRIBE OUTPUT` need small extensions in the same style as `alter_execute.rs`.
2. Validate the `PREPARE` body by parsing it (and planning it with `number_placeholders`, which `describe_prepared` already does) before echoing `X-Trino-Added-Prepare`. Reject unknown `SET SESSION` properties with Trino's `INVALID_SESSION_PROPERTY` error; keep an allow-list of the properties SQE honours (`iceberg.compression_codec` is the one used today, `server.rs:653-667`).
3. Apply `X-Trino-Set-Catalog`/`Schema` only on the terminal `Finished` response (`get_queued_results` already does this; remove the early application in `submit_query`).
4. Bind `EXECUTE ... USING` parameters on the AST (`Value::Placeholder` substitution) instead of text splicing, or at minimum make `substitute_placeholders` comment-aware and share one implementation with the Flight `DoPutPreparedStatement` path.

## Acceptance criteria

- Unit tests: `PREPARE p FROM SELEC 1` returns a `SYNTAX_ERROR` on the POST; `-- comment\nUSE x` is recognized; `SET SESSION bogus = 1` returns `INVALID_SESSION_PROPERTY`.
- Unit test: a `USE` that fails after the bounded wait never emits `X-Trino-Set-Schema`.
- Unit test: `EXECUTE p USING 1` where `p` is `SELECT ? /* why? */` binds exactly one parameter.
- `protocol.rs` and `prepared.rs` contain no `to_uppercase()`-and-`starts_with` statement detection.

## Related

- 700 (single SQL front door; these become statement kinds there).
- 100-range findings on `sqe-sql` custom statements.
- `docs/internal/reviews/2026-06-26-codebase-analysis.md` issue #10 (`todo!()` in prepared.rs) is fixed; the replacement is what this issue describes.
