# 707: The bench harness rewrites benchmark SQL with string heuristics to qualify table names instead of setting the session schema

- Scope: sqe-bench (query.rs, comparison/mod.rs), benchmarks/queries
- Severity: medium
- Category: testing
- Effort: S (hours)
- Status: open

## Problem

Every benchmark query is rewritten before execution to prefix bare table names with the namespace, `crates/sqe-bench/src/query.rs:174-260` (`prefix_tables`). The function strips line comments, then for each table name (longest first) does a substring `find`, checks byte-level word boundaries, and guesses table context from the preceding text:

```rust
let in_table_context = upper_before.ends_with(" FROM")
    || upper_before.ends_with(" JOIN")
    || upper_before.ends_with(" TABLE")
    || upper_before.ends_with(" INTO")
    || upper_before.ends_with(" UPDATE")
    || upper_before.ends_with("\nUPDATE")
    || upper_before.ends_with(" EXISTS")
    || trimmed_before.ends_with(',')
```

The comment at `query.rs:252-253` records the known failure mode and the chosen workaround:

```
// Note: this can over-qualify column aliases that share names with tables.
// Such queries are fixed by renaming the conflicting aliases in the query files.
```

So the committed TPC-H/TPC-DS/SSB query text in `benchmarks/queries/` has been edited away from the canonical specification to suit the rewriter. `comparison/mod.rs:62-70` applies the same rewrite to the SQL sent to Trino, so Trino also runs the edited text.

Both engines already support a default schema without any SQL rewriting: the Trino protocol has `X-Trino-Schema` (the server applies it, `crates/sqe-trino-compat/src/server.rs:774-780`), and Flight SQL sessions accept `USE <namespace>`. The bench Trino client sets only `X-Trino-Catalog` (`client/trino.rs:72-74`).

## Why it matters

The compare harness is the engine's correctness oracle. A heuristic rewrite of the oracle's input is a source of false diffs and false matches that has already cost investigation time (the memory of "Trino compare silent skip" and "compare known artifacts" both stem from harness behavior, not engine behavior). Editing canonical query text means published benchmark numbers are not for the standard queries. `sqlparser` is already a workspace dependency; a tokenizer-level qualifier would be exact, but the better fix is to not rewrite at all.

## Proposed change

1. Stop rewriting. In `run_comparison` and the `test`/`run` verbs, set the default schema on the connection: Trino client sends `X-Trino-Schema: <namespace>`; Flight client issues `USE <catalog>.<namespace>` once after connect (or sets the session default via the Flight SQL session-options RPC if the coordinator supports it).
2. Restore the canonical query text in `benchmarks/queries/` from the TPC specifications (or the dbgen/dsqgen templates) and record the source in a `README` next to them.
3. Keep `prefix_tables` only behind an explicit `--qualify-tables` flag for engines that cannot set a default schema, and move it to the lib so it can be tested from `tests/` (see 706). Give it a tokenizer-based implementation using `sqlparser::tokenizer` so aliases and literals are never touched.

## Acceptance criteria

- `sqe-bench compare tpch --scale 1` runs with `prefix_tables` disabled and produces the same 22/22 match count as the current baseline.
- `git diff` of `benchmarks/queries/tpch/*.sql` against the TPC-H 3.0.1 reference queries shows only substitution-parameter differences.
- A unit test proves `prefix_tables` (if retained) leaves `SELECT part AS p FROM lineitem` with a column alias `part` untouched.

## Related

- 705 (shared Trino client would carry the `X-Trino-Schema` header).
- 706 (bin/lib split blocks integration tests of this code).
- Memory: Trino compare silent skip; compare known artifacts.
