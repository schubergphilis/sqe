## Context

`QueryHandler` (`crates/sqe-coordinator/src/query_handler.rs`) is 13,172 lines and 309 functions. `execute_inner` has this shape today:

| part | lines | content |
|---|---|---|
| preflight | 340 | size check, audit-closed check, admission, classification, pressure refusal, transaction and branch guards, sorted-write permit, reserved-property check, span fields, query id, tracker start, abandoned-query guard, OpenLineage start, result-cache lookup |
| dispatch | 1,250 | one `async` block holding `match &kind` with about 75 arms and no wildcard |
| completion | 460 | `select!` against timeout and cancellation, row counts, audit resources, span fields, metrics, memory accounting, OpenLineage complete, audit record, slow-query log |

Most dispatch arms are one line that calls `handle_*`. Six hold logic inline: `Ctas` 150 lines, `Procedure` 144, `Utility` 140, `Insert` 95, `Merge` 79, `Drop` 79.

Two locals are written inside dispatch and read in completion: `captured_plan` and `policy_summary`. Ten arms already pass them down as `&mut`. Three assign `captured_plan` directly.

Constraints: one logical change per merge request; the local integration suite is the gate, CI has no equivalent; `sqe-coordinator` takes minutes to rebuild, so every step must be verifiable with a narrow test filter.

## Goals / Non-Goals

**Goals:**

- No function on the statement path longer than a reviewer can read in one sitting.
- Each stage testable alone.
- A diff per step that a reviewer can verify as a move.
- A ratchet so the result holds.

**Non-Goals:**

- Faster compiles. That work is separate and profile-driven.
- Splitting `sqe-coordinator` into more crates. An earlier split gave no compile gain.
- Any change to statement semantics, error codes, audit content or metrics.
- Regrouping `StatementKind` into nested enums.

## Decisions

**1. Three stages and a run context, not a framework.**
`execute_inner` keeps its signature and becomes a short function that calls `preflight`, `dispatch`, `complete`. A `StatementRun` struct carries what the stages share: query id, start time, kind name, cancel token, query span, admission, `captured_plan`, `policy_summary`, plan metrics, the OpenLineage flag. The alternative, a trait per stage with pluggable observers, adds indirection for a pipeline that has exactly one implementation.

**2. The match stays flat and exhaustive.**
An earlier draft routed by statement family first (query, DDL, DML, access control) and matched again inside each family. The match has no wildcard arm today. A family router over the flat enum needs one wildcard per family, and a new statement kind would then compile without a handler. Families become files of `impl QueryHandler` blocks (`statement/ddl.rs`, `statement/write.rs`, and so on). Routing stays in one place.

**3. Shared state flows through `&mut`, never through the return value.**
Completion reads `captured_plan` after the `select!`. A statement that times out or is cancelled still audits the tables it touched, because the handler wrote the plan before the future was dropped. A handler that returned the plan with its result would lose it on timeout. Lifted handlers take `&mut StatementRun` or the two `&mut Option<_>` fields, matching the ten arms that already do.

**4. Guards live in the run context and drop with it.**
`admission`, the sorted-write permit, `AbandonedQueryGuard` and the cancel token are fields of `StatementRun` or locals of `execute_inner`, declared in today's order. None moves into a stage function's local scope, where it would drop when that stage returns.

**5. One audit record per statement.**
Before the tracker starts, failures are audited by the `failed` closure. After it starts, completion audits. `preflight` returns either a run context (tracker started) or an error that has already been audited. The type makes the second case explicit so the caller cannot audit it again.

**6. Move first, improve never (in the same merge request).**
Each lift is a pure move: cut the arm body, paste it as a function, replace captured locals with parameters. `git diff --color-moved=zebra --color-moved-ws=allow-indentation-change` must show the body as moved. Cleanups go in a later merge request.

**7. Stage splits use the existing seams.**
Preflight ends where the result-cache lookup ends; completion starts at the `select!`. Splitting there closes one function and opens the next without moving text, so the first merge request is small.

**8. Function length is ratcheted by `xtask budgets`.**
`budgets.json` gains a `functions` section: a default limit and a list of grandfathered functions with their current length. A grandfathered function may shrink (the tool asks for the entry to be lowered) and may not grow. Measured with `syn` on the parsed item, which `xtask` already depends on or can take cheaply. `clippy::too_many_lines` was considered: it cannot express a per-function ceiling that only moves down.

**9. The shadow list is a test, not a document.**
A test builds a default DataFusion `SessionContext` and SQE's, and lists the scalar, aggregate and window names SQE registers over a built-in. Each must appear in an allowlist with a one-line reason (for example Trino date semantics). An unlisted shadow fails the test, so a DataFusion upgrade that adds a built-in SQE already defines is noticed.

**10. The vendor ledger is generated, then annotated.**
For each tree under `vendor/`, diff against the registry copy of its base version, one entry per hunk group: file, purpose, upstream status (`ours`, `upstream in <version>`, `superseded`). Entries marked upstream are dropped at the next re-vendor.

## Risks / Trade-offs

- [A lifted arm changes when a guard drops or an error is audited] → the invariants are requirements in `specs/statement-pipeline`; each lift runs the integration filter for that statement kind; audit-count assertions are added before the first lift.
- [Splitting an `async fn` adds `Send` proofs and slows the build] → profile `sqe-coordinator` before and after the first lift, back to back; box the handler only if the profile bills it.
- [Many small merge requests conflict with feature work in the same file] → one arm per merge request, merged the same day; no long-lived branch.
- [The access-control suite is red on `main`] → the access-control arms and `handle_grant*` are not moved until `insert_does_not_confer_storage_relocation` passes again.
- [The ratchet blocks an urgent fix that lengthens a grandfathered function] → the entry can be raised in the same merge request, visibly, with a reason.

## Migration Plan

No deployment step. Each merge request is behaviour-neutral and can be reverted alone. Order: audit-count tests, stage split, six lifts, completion functions, then `sqe-write` and `sqe-catalog`, then the ratchet with the then-current list.

## Open Questions

- Where the registry test lives: `sqe-coordinator` sees the final registry, `sqe-trino-functions` owns most shadows.
- Whether `iceberg_scan.rs` `execute` (1,123 lines) is stage-shaped or needs its own design note.
