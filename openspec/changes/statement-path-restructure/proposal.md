## Why

The coordinator's statement path is one function. `QueryHandler::execute_inner` is 2,082 lines: 340 lines of preflight, a 1,250-line match over about 75 statement kinds, and 460 lines of audit, metrics and lineage. A reviewer cannot hold it, a unit test cannot reach one stage without the other two, and every statement kind added makes it longer. 72 production functions are 150 lines or longer (22,000 lines in total) and nothing stops the next one.

The same review found two places where SQE carries code that upstream may now own: SQL functions registered under a name DataFusion also defines, and patches in `vendor/` with no record of which ones have landed upstream.

## What Changes

- `execute_inner` becomes three named stages (preflight, dispatch, completion) that share one run context. Behaviour is unchanged.
- The six dispatch arms that hold their logic inline (`Ctas`, `Procedure`, `Utility`, `Insert`, `Merge`, `Drop`, about 690 lines) move into `handle_*` functions, the pattern the other arms already use. The match stays flat and exhaustive.
- Completion (audit, metrics, lineage, slow-query log) becomes separate functions over one outcome value.
- The same treatment for `write_handler.rs` (`handle_merge_equality`, `handle_merge`, `handle_ctas_streaming_on`, `handle_delete_mor`) and `iceberg_scan.rs` `execute`, after the coordinator pilot.
- A function-length budget joins the file and crate budgets. Existing long functions are recorded; they may shrink but not grow, and no new one may be added.
- A test compares SQE's function registry with a default DataFusion registry. Every name SQE shadows needs an entry that says why.
- A ledger lists every local patch in `vendor/` against its upstream base and states whether it has landed upstream.

No user-visible change. No SQL, configuration or wire-protocol change.

## Capabilities

### New Capabilities

- `statement-pipeline`: the invariants of the coordinator statement path that hold across the restructure: audit exactly once, guard lifetimes, exhaustive dispatch, partial lineage on timeout.
- `code-size-budgets`: function-length budget enforced with the existing file and crate budgets.
- `upstream-parity`: the function-registry shadow check and the vendored-patch ledger.

### Modified Capabilities

None.

## Impact

- `crates/sqe-coordinator/src/query_handler.rs` and new sibling modules for the lifted arms and the completion stage.
- `crates/sqe-write/src/write_handler.rs`, `crates/sqe-catalog/src/iceberg_scan.rs` in the second phase.
- `xtask/src/budgets.rs`, `.config/budgets.json`.
- A new test in `sqe-coordinator` (or `sqe-trino-functions`) for the registry check; `vendor/PATCHES.md` for the ledger.
- No dependency change. No compile-time gain is claimed: `QueryHandler::execute` is already a boxed future. Boxing is added only where `scripts/compile-profile.sh` attributes cost.
