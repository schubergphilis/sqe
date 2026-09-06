# 700: Trino dialect normalization is hand-assembled differently at three entry points

- Scope: sqe-trino-compat (server.rs), sqe-coordinator (query_handler.rs), sqe-cli (embedded.rs), sqe-sql rewriters
- Severity: high
- Category: architecture
- Effort: M (days)
- Status: open
- Issue: #620

## Problem

The same SQL text is normalized by a different subset of rewriters depending on which door it comes through. There is no single "SQL front door" type; each caller lists the rewrites it remembered.

Trino HTTP path, `crates/sqe-trino-compat/src/server.rs:1207-1246`, applies five source-level rewrites before the executor sees the SQL:

```rust
let effective_sql = sqe_sql::rewrite_paren_less_values(&effective_sql);
let effective_sql = sqe_sql::rewrite_bare_table(&effective_sql);
let effective_sql = sqe_sql::rewrite_nested_row_cast(&effective_sql);
let effective_sql = match session.default_catalog.as_deref() {
    Some(cat) if info_schema_compat::is_metadata_query(&effective_sql) => {
        info_schema_compat::qualify_information_schema(&effective_sql, cat)
    }
    _ => effective_sql,
};
let effective_sql = sqe_sql::alias_anonymous_select_columns(&effective_sql);
```

Coordinator path (shared by Flight SQL and the Trino executor), `crates/sqe-coordinator/src/query_handler.rs:3093` and `:3462`, applies only the AST rewriter:

```rust
let sql = sqe_sql::rewrite_trino_compat(&sql);
```

Embedded CLI path, `crates/sqe-cli/src/embedded.rs:826-836`, applies neither set. It runs `rewrite_hf_urls_in_sql` and `rewrite_named_tvf_args` and hands the text to `ctx.sql` directly, with the comment "exactly as the coordinator does" for one of them.

Consequences that follow directly from the code:

- `INSERT INTO t VALUES 1`, `TABLE nation`, and nested `CAST(ROW(..) AS ROW(..))` work over Trino HTTP and fail over Flight SQL (dbt-sqe, ADBC) and in `sqe-cli --embedded`.
- Column naming differs by protocol: `SELECT 1+1` returns `_col0` over Trino HTTP and `Int64(1) + Int64(1)` over Flight (`sqe-sql/src/trino_compat.rs:285-318` documents the Trino-only scoping as deliberate, but nothing documents why Flight clients should see a different name for the same query).
- Each source-level rewriter parses the statement again on failure-to-parse gating (`bare_table.rs:35-45`, `nested_row_cast.rs:39-52`, `paren_less_values.rs`), so a Trino-path statement is parsed up to seven times before DataFusion parses it once more.
- The ordering is load-bearing (`nested_row_cast` must run before the AST rewriter; `alias_anonymous_select_columns` must run before DESCRIBE interception) and exists only as call order in one function.

## Why it matters

Every dialect fix lands in one door and silently misses the other two. The memory of issue #337 (identifier folding) and the memory of DF54 named-TVF-arg regression both show fixes applied in one place then re-discovered elsewhere. dbt-sqe over Flight and dbt-trino over HTTP are both supported adapters and today they do not accept the same SQL. The embedded CLI is the documented quickstart and it diverges further.

## Proposed change

Introduce one type in `sqe-sql`, for example `pub struct SqlFrontDoor { dialect: WireDialect }` with `fn normalize(&self, sql: &str) -> Result<NormalizedSql, SqlError>`, that owns the ordered rewrite chain and parses once:

1. Tokenize/parse once with the DuckDB dialect. Run the source-level fallbacks (`paren_less_values`, `bare_table`, `nested_row_cast`) only when that first parse fails, then parse once more.
2. Run the AST rewriters (`rewrite_trino_compat`, `alias_anonymous_select_columns`, information_schema qualification as an AST pass over `ObjectName`) on the single AST.
3. Return the AST plus the serialized text so `parse_and_classify` and DataFusion do not re-tokenize.

`WireDialect::Trino` versus `WireDialect::Flight` selects only what is genuinely protocol-specific (currently: nothing that has been justified; `_colN` aliasing should apply to both or be documented). Call it from `query_handler.rs`, the Trino `submit_query`, and `EmbeddedClient::execute`. Delete the per-caller chains.

## Acceptance criteria

- A table-driven test in `sqe-sql` runs the same set of Trino-shape statements through `SqlFrontDoor` for both dialects and asserts identical normalized output.
- `crates/sqe-cli/src/embedded.rs` and `crates/sqe-trino-compat/src/server.rs` contain no direct calls to individual `rewrite_*` functions.
- An integration test executes `INSERT INTO t VALUES 1`, `TABLE t`, and `SELECT 1+1` over Flight SQL and Trino HTTP and asserts the same result and column name.
- A single parse per statement is verifiable by a counter in a test build, or by a `tracing` span count.

## Related

- 704 (metadata post-processing keyed on SQL substring; becomes an AST pass under this change)
- 708 (session statements parsed by prefix; same front door)
- `docs/internal/reviews/2026-07-25-production-readiness-review.md` item 15 (Trino compat integration tests) still holds.
- Memory: DF54 named TVF arg regression fixed by positional rewrite (!380) was applied to coordinator and CLI separately.
