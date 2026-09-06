# 106: The SQL rewrite chain is assembled differently by each entry point and re-parses the same text three to five times

- Scope: sqe-sql, with call sites in sqe-coordinator, sqe-trino-compat, sqe-mcp
- Severity: medium
- Category: architecture
- Effort: M (days)
- Status: open
- Issue: #549

## Problem

`sqe-sql` exposes eleven text-in, text-out rewriters plus two spec extractors,
and each protocol front end composes its own subset in its own order.

`pre_parse_pipeline` (`crates/sqe-sql/src/pipeline_types.rs:69-90`) covers
`extract_incremental_spec`, `extract_time_travel_spec`,
`normalize_partitioned_by`, `rewrite_ctas_compat`, `rewrite_alter_execute`,
`rewrite_view_compat`. Everything else is stitched by callers:

- Flight SQL: `pre_parse_pipeline` then classify (`sqe-coordinator/src/flight_sql.rs:876-878`);
  the execute path then runs the pipeline again, then `rewrite_trino_compat`
  and `rewrite_named_tvf_args` (`query_handler.rs:3093-3094`, `:3462-3463`),
  then `ctx.sql(...)`.
- Trino HTTP: additionally `rewrite_paren_less_values`, `rewrite_bare_table`,
  `rewrite_nested_row_cast` and the information_schema qualifier
  (`sqe-trino-compat/src/server.rs:1247-1270`) before handing the string to
  the same coordinator path; `alias_anonymous_select_columns` is documented as
  Trino-only (`trino_compat.rs:290`).
- MCP: `pre_parse_pipeline` and classify only (`sqe-mcp/src/lib.rs:621-622`).
- CLI embedded: `parse_and_classify` on raw text, then `rewrite_named_tvf_args`
  (`sqe-cli/src/embedded.rs:744,834`).

Each rewriter is self-contained: it calls `Parser::parse_sql` itself, walks
the AST, and if anything changed re-serialises the whole statement list with
`Display` and joins with `"; "` (`trino_compat.rs:129-133`, `:255-262`;
`tvf_named_args.rs:61-82`; same shape in `ctas_compat.rs`, `alter_execute.rs`,
`view_compat.rs`, `paren_less_values.rs`, `bare_table.rs`,
`nested_row_cast.rs`). On parse failure every one of them returns the input
unchanged (`Err(_) => return sql.to_string()` at `trino_compat.rs:133`,
`:289`, `tvf_named_args.rs:65`). A Flight SQL `SELECT` is therefore parsed by
sqlparser at least four times (classify, trino_compat, named_tvf_args,
DataFusion's own parse) and Display-roundtripped up to twice before planning.

`ClassifiableSql::from_normalized` (`pipeline_types.rs:56`) is `pub`, so the
newtype that the module header says "gives the compiler a way to enforce the
stage order" can be constructed from any string; the Trino server and the CLI
do exactly that by never using it.

## Why it matters

Behaviour differs by wire protocol for the same SQL and the same user.
`VALUES 1, 2` and `TABLE t` work over Trino HTTP and fail over Flight SQL and
MCP; `SELECT 1+1` gets `_col0` over Trino and `Int64(1) + Int64(1)` over
Flight. dbt-sqe (Flight SQL) and a BI tool on the Trino port disagree about
what is valid. Every new rewriter has to be added in up to four places, and
issue #337 (identifier folding) and #9/#315/#335/#351c each landed in one.

Classification happens on a different string than execution: the classifier
sees pre-trino-compat text, the planner sees post-rewrite text, so a rewriter
can change the statement kind after routing has been decided. The
"return input on parse error" rule means a rewriter that fails to parse
something the classifier accepted silently skips, and the downstream error
names DataFusion's parser rather than the real cause.

Display round-tripping through sqlparser is lossy for some constructs (the
code says so at `trino_compat.rs:250-253` and `tvf_named_args.rs:70-73`) and
the number of round trips depends on which rewriters fired. Parsing the same
statement four times is also measurable on short-query workloads.

## Proposed change

1. Define the pipeline once in `sqe-sql`: `pub fn prepare(sql: &UserSql,
   dialect: FrontEnd) -> Result<PreparedStatement>` where `PreparedStatement`
   holds the single parsed `Statement`, the extracted `TimeTravelSpec` /
   `IncrementalSpec`, and the `StatementKind`. `FrontEnd::{FlightSql, TrinoHttp,
   Mcp, Cli}` selects only the front-end-specific cosmetic rewrites (column
   aliasing); every syntax-acceptance rewrite runs for all front ends.
2. Convert the AST rewriters from `fn(&str) -> String` to
   `fn(&mut Vec<Statement>) -> bool` visitors over one parsed tree. Keep the
   handful of genuinely pre-lexical rewrites (`FOR VERSION AS OF`,
   `PARTITIONED BY`, nested `row(...)` types) as the only text stages, run
   once, before the single parse.
3. Serialise to text once at the end for `ctx.sql`, or better, hand DataFusion
   the `Statement` directly via `SessionState::statement_to_plan`, which removes
   the last re-parse.
4. Make `ClassifiableSql::from_normalized` `pub(crate)` and delete the three
   external constructions.

## Acceptance criteria

- One function in `sqe-sql` is the only caller of `Parser::parse_sql` on user
  text in the non-test build: `grep -rn 'Parser::parse_sql' crates --include=*.rs | grep -v test`
  shows `sqe-sql` only.
- A cross-protocol parity test in `make test-integration` runs the same
  statement list (`VALUES 1`, `TABLE t`, `SELECT 1+1`, a `$snapshots` query, a
  named-arg TVF, a `WITH NO DATA` CTAS) over Flight SQL, Trino HTTP and MCP and
  asserts identical success and identical row values (column names may differ
  only where `FrontEnd` says so).
- `rewrite_trino_compat` and `rewrite_named_tvf_args` are no longer called
  from `sqe-coordinator/src/query_handler.rs`.

## Related

- `107-classifier-prefix-dispatch-and-multi-statement-truncation.md`
- `105-unicode-uppercase-byte-slice-panics-in-sql-prescans.md`
- Scope 700 (sqe-trino-compat) may report the Trino-side half; this file owns
  the `sqe-sql` API shape.
- Overlaps `700-trino-dialect-pipeline-split-across-three-entry-points.md`, which reports the same split from the Trino side; one MR should close both.
