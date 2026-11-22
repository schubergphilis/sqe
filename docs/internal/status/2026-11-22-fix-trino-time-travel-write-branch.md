# fix/trino-time-travel-write-branch

**Date:** 2026-11-22

The independent Iceberg matrix suite found two statements that work over Flight SQL but fail over Trino HTTP:

- **Time travel.** `FOR VERSION|TIMESTAMP AS OF` and `FOR INCREMENTAL BETWEEN` were rejected by the parser in `resolve_statement` before anything stripped the clause.
  - The clause is now stripped only to classify the statement. The executor gets the original SQL and resolves it the same way it does for Flight SQL.
- **`SET WRITE_BRANCH`.** It needed a session held by the session manager, and the stateless Trino path has none.
  - It is now the allow-listed session property `iceberg.write_branch`. SQE echoes it in `X-Trino-Set-Session` (or `X-Trino-Clear-Session` for RESET), the client replays it in `X-Trino-Session`, and `apply_trino_headers` puts it on the session.
  - The coordinator checks the effective write branch with `reject_txn_ref`, so a client-held value cannot name a transaction branch.

An end-to-end probe over Trino HTTP against an image built from this branch found two older defects, both fixed here:

- **`USE` failed** with "USE: session not found in manager". The Trino layer hands `USE` to the executor (its headers go out only if execution succeeds), and the coordinator tried to switch a stored session that a Trino request never has. The coordinator does not check that the schema exists on either protocol, which Trino does; that is the next branch below. It now leaves the scope to the protocol layer when the manager does not hold the session, the same way COMMIT and ROLLBACK already do.
- **Unqualified time travel ignored the session schema** on both protocols: a bare `t FOR VERSION AS OF` always read `default.t`. The policy rewriter already qualified the restored scan with the session schema (`TableScope::of(ctx)`), so before this fix a bare-name time-travel query read `default.t` under the row filters and masks of `sales.t`. `split_time_travel_name` now qualifies the name with that same `TableScope::of(ctx).qualify(..)`, so the table that is read and the table whose policies apply cannot diverge; a unit test pins the equality.

Evidence:
- `cargo test -p sqe-trino-compat --lib`: 200 passed.
- `check-server` and clippy `-D warnings` are clean.
- Matrix suite on a Linux image built from this branch: `time-travel` and `branching-tagging` are full for v2 and v3, and there are no protocol gaps.

**NEXT:**
- `fix/use-schema-must-exist`: `USE` of a schema that does not exist succeeds on both protocols (Trino fails it with SCHEMA_NOT_FOUND). Check the namespace in the coordinator's USE arm. `late_failing_use_never_switches_the_client` already models the expected error.
- Make the matrix suite (!1929) assert writing to a branch over Trino positively. Today it only stopped reporting the gap.
