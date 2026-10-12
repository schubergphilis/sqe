**STATUS 2026-09-08 (evening): finding 205 (#558) on `fix/show-stats-restricted-columns-205`: `SHOW STATS` is governed by the same effective policy as a scan.**

`handle_show_stats` read the Iceberg schema directly and listed every
column, and reported the snapshot's `total-records` regardless of the
caller's row filter; `information_schema.columns` (and so `SHOW COLUMNS`,
`DESCRIBE`, `SHOW CREATE TABLE`) hid restricted columns. It now resolves the
caller's effective policy with the rewriter's key rule
(`resolve_policy_key_for_ident`) and `resolve_effective_policy` (resource
policy plus the table's `sqe.column-tags`, malformed tags fail closed), and
the pure `governed_show_stats` drops restricted columns, reports the row
count NULL under any row filter, and answers a deny-all policy with no
rows. Masked columns stay listed; a comment at the gate says per-column
statistics must come from the enforced plan when Puffin NDV/min-max land
there. The MCP `get_table_stats` tool inherits this. Tests: four unit tests
on the rule; `tests/it/show_stats_policy_e2e.rs` (`needs:polaris`) runs the
same table through a passthrough handler and a handler wired to an
`InMemoryPolicyStore` (new `common::setup_handler_with_policy_store`) and
checks the column list, the NULL count and the filtered `count(*)`. Gates:
`it` 150 + the live case, clippy clean, `test-groups --check`.
**NEXT:** 610 (#657), 627, #659 partials; the docker-capable CI runner for
the `polaris` group (807 item 3).
