# 205: SHOW STATS lists restricted column names that information_schema hides

- Scope: sqe-coordinator statement routing, sqe-mcp `get_table_stats` tool
- Severity: medium
- Category: security
- Effort: S (hours)
- Status: open

## Problem

`information_schema.columns` filters restricted columns so they are invisible
(`crates/sqe-catalog/src/info_schema.rs:233-236`), and `SHOW COLUMNS`,
`DESCRIBE` and `SHOW CREATE TABLE` all read from it
(`crates/sqe-coordinator/src/query_handler.rs:4703-4740`).

`SHOW STATS` reads the Iceberg schema directly:

```rust
// crates/sqe-coordinator/src/query_handler.rs:6425-6437
let column_names: Vec<String> = metadata.current_schema().as_struct().fields()
    .iter().map(|f| f.name.clone()).collect();
build_show_stats_batch(&column_names, row_count)
```

No `PolicyStore::resolve` call, no restricted-column filter. It also returns
the table row count from the snapshot summary regardless of the caller's row
filter. The MCP `get_table_stats` tool is a thin wrapper over this statement
(`crates/sqe-mcp/src/lib.rs:562`).

## Why it matters

CLAUDE.md states "Denied columns are invisible (not errors)". A user can
enumerate restricted column names with `SHOW STATS FOR hr.employees`, and
learn the true total row count of a table they only see a slice of. Neither
is a value leak today (per-column stats are NULL), but the moment per-column
Puffin NDV/min/max stats are surfaced in this statement (memory note "Puffin
NDV consumer" is in flight), min/max of a restricted or masked column becomes
a direct data leak through an MCP tool that agents will call routinely.

## Proposed change

1. In `handle_show_stats`, resolve the caller's policy with the same key
   function as the rewriter (see 201) and drop restricted columns from
   `column_names`. When the policy carries a `lit(false)` deny
   (`sqe_policy::plan_rewriter::policy_denies_all`), return an empty result or
   the same error the rewriter path would produce.
2. Report `row_count` as NULL when the resolved policy has any row filter, or
   compute it through the governed plan (`SELECT count(*)`), never from the
   snapshot summary.
3. When per-column stats land, source them from the enforced plan or suppress
   them for masked and restricted columns.
4. Add a comment at the top of `handle_show_stats` naming this rule so the
   Puffin work does not reintroduce the leak.

## Acceptance criteria

- Integration test: table with a restricted column; `SHOW STATS FOR t` as the
  restricted user does not list the column; as an unrestricted user it does.
- Test: user with a row filter sees `row_count` NULL (or the filtered count).
- MCP `get_table_stats` test in `crates/sqe-mcp` using a recording executor
  asserts the SQL is `SHOW STATS FOR "c"."s"."t"` (already the case) and the
  coordinator-level test above covers the enforcement.

## Related

- 201 (shared key function).
- memory note "Puffin NDV consumer".
