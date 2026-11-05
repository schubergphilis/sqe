**STATUS 2026-11-05: per-catalog identifier case for Snowflake Horizon (`feat/catalog-identifier-case`, follow-up to #337).**

`[catalogs.<name>] identifier_case = "upper"` (config) and
`ATTACH ... (IDENTIFIER_CASE 'upper')` (coordinator runtime registry and the
embedded CLI) fold unquoted identifiers to upper case for that catalog in the
front door (`sqe-sql/src/identifier_case.rs`), before planning and policy.
Output idents are quoted upper case, so DataFusion's lower-casing does not
undo them and a second fold is a no-op. Scope: table names in upper catalogs
always; columns, aliases, CTE names only when every relation is upper (mixed
joins, TVFs and `$` metadata tables fold table names only). One- and
two-part names resolve through the session catalog, else the default catalog.
`USE sf.sales` / `USE sales` folds its schema part too (the coordinator's
`parse_use_target` keeps quoted case), so later one-part names resolve.

Live probe against Horizon (SQE_PROBE schema, read-only): embedded ATTACH with
the option (unquoted table/columns, alias, CTE + GROUP BY, hand-quoted form,
quoted SQE-written lower table, `$snapshots`; unquoted lower table fails; the
control without the option fails) and a coordinator with the config option
(3-part read, USE + one-part name, USE + join of an upper and a quoted lower
table, unquoted lower table fails). All as expected.

Policy: masks bind case-insensitively (`ResolvedPolicy::mask_for`), so a mask
on `ssn` covers `SSN`. Row-filter expressions are not folded; an unquoted
`region` against `REGION` fails the query (tested, fails closed). Tests:
sqe-sql unit tests, `sqe-policy/tests/it/identifier_case_policy.rs`
(folded == hand-quoted under mask + row filter), config parse test.

Limits: SQE-written lower-case tables in an upper catalog need quotes in SQE,
symmetric to Snowflake. The Snowflake interop harness does not set the
option; its last probe (unquoted name must fail) inverts if it ever does.

**NEXT:** `make test-integration` not run for this branch; `$files` delete-file listing question still open.
