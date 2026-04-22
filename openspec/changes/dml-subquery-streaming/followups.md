# Follow-up issues

The repo is hosted on a private GitLab, not on GitHub, so the two follow-up
issues for this change are captured here as ready-to-file drafts. Open them in
GitLab once the change has been merged. Reference this OpenSpec change
(`dml-subquery-streaming`) and the commits that land it.

## 1. Revert 8 MiB thread_stack_size after one release in production

**Title:** Revert `WORKER_STACK_BYTES = 8 * 1024 * 1024` in `sqe-coordinator/src/main.rs`

**Body:**

The 8 MiB tokio worker stack was added as defence in depth against the CoW DML
stack overflow that the `dml-subquery-streaming` change just fixed. With the
view-lifted IN-subquery rewriter in place, no SQE code path produces deep
expression trees from IN subqueries, so the larger stack is no longer needed.

Once `dml-subquery-streaming` has been in production for one full release with
no regression, revert the stack bump back to the tokio default (2 MiB) and let
the regression tests at `crates/sqe-coordinator/tests/in_subquery_or_stack_overflow.rs`
+ `tests/in_subquery_view_rewrite.rs` carry the load.

**Files:**

- `crates/sqe-coordinator/src/main.rs:85-99`: drop the `thread_stack_size` call.
- `crates/sqe-coordinator/tests/in_subquery_or_stack_overflow.rs`: keep as a
  regression gate (the test exercises DataFusion directly, not SQE's rewriter).

**Acceptance:**

- Coordinator runs the standard tokio default stack.
- TPC-E SF10 + SF100 `trade_result_update_holding` continue to pass.
- `cargo test --test in_subquery_or_stack_overflow prod_stack_8k` still passes
  (one rung lower than the current ceiling. The test was always a ladder, not
  a hard guarantee.).

## 2. Optional `dml.in_subquery.max_materialised_rows` config knob

**Title:** Add `dml.in_subquery.max_materialised_rows` config knob with view-based fallback

**Body:**

`lift_in_subqueries` materialises the deduplicated subquery into a scratch
MemTable in coordinator memory. For 100M+ row subqueries this can exceed
available RAM. We currently have no upper bound; if a user submits an UPDATE
with a 1B-row IN subquery the coordinator will OOM.

Add an optional config knob:

```toml
[dml.in_subquery]
max_materialised_rows = 10_000_000  # default
fallback = "view"                   # or "error"
```

When the materialiser would produce more rows than the cap, either:
- `fallback = "view"`: register the subquery as a temporary VIEW (not a
  materialised MemTable) and let DataFusion stream the join. Works around the
  memory cap at the cost of re-executing the subquery once per CoW data file.
- `fallback = "error"`: surface a clear error message asking the user to
  narrow the subquery or pre-stage the keyset.

**Files:**

- `crates/sqe-core/src/config.rs`: add the new config struct.
- `crates/sqe-coordinator/src/write_handler.rs`: preflight the subquery's
  estimated row count via DataFusion's statistics, then choose the path.

**Acceptance:**

- Default behaviour unchanged for subqueries under 10M rows.
- A 100M-row IN subquery with `fallback = "view"` completes without OOM (may
  be slower than materialised).
- A 100M-row IN subquery with `fallback = "error"` returns a clear error.

## 3. Replace MemTable materialisation with a LeftSemi join rewrite

**Title:** Rewrite `lift_in_subqueries` to emit `LogicalPlan::Join { join_type: LeftSemi }` instead of materialising a scratch MemTable

**Body:**

Survey of comparable open-source engines (captured while debugging the SF10
regression):

- **DuckDB** uses `JoinType::MARK` for `IN` / `NOT IN`. The subquery becomes the
  right side of a mark join; a boolean "matched" column is projected back into
  the outer plan. No materialisation. Source: `src/planner/subquery/*`.
- **RisingWave** applies `ApplyToJoinRule` in the optimiser, turning the
  correlated / uncorrelated IN into a `LogicalJoin` (semi or anti depending on
  polarity). Source: `frontend/src/optimizer/rule/apply_to_join.rs`.
- **Trino** has `TransformUncorrelatedInPredicateSubqueryToSemiJoin`. A single
  semi-join node replaces the `InPredicate`; the physical planner picks hash,
  broadcast or partitioned semi-join based on statistics.

All three share one principle: one relational operator, streamed, no
materialisation, no RAM ceiling. The current SQE approach materialises the
deduplicated keyset into a MemTable and emits a `LEFT JOIN ... COALESCE(..., FALSE)`
pattern. It works for SF10 and unblocks TPC-E, but it has two costs:

1. Memory: the scratch MemTable pins every unique key in RAM for the lifetime
   of the DML batch loop (the RAII guard releases it when the handler exits).
2. Per-batch overhead: each CoW data-file batch re-runs the LEFT JOIN against
   the scratch table. DataFusion does hash-join this efficiently, but the same
   work would happen once in a streaming semi-join.

**Proposed change:**

Replace `lift_in_subqueries` (post-parse string rewrite) with a LogicalPlan
rewrite that lowers `Expr::InSubquery` into a `LogicalPlan::Join { join_type:
LeftSemi, .. }` for the affirmative case and `LeftAnti` for `NOT IN`. The
rewrite runs once, returns a new `LogicalPlan`, and lets DataFusion's optimizer
pick the join implementation.

This **supersedes follow-up #2**: once the rewrite uses a streaming semi-join,
DataFusion's hash-join spills to disk natively when the build side does not
fit in memory, so the `max_materialised_rows` config knob stops being
necessary. Keep #2 open only if we decide to ship the knob as a stopgap
before landing this rewrite.

**Files:**

- `crates/sqe-coordinator/src/write_handler.rs`: delete `lift_in_subqueries`
  and the `InSubqueryCleanup` guard; route the WHERE clause through a new
  `rewrite_in_subquery_to_semi_join(plan: LogicalPlan) -> LogicalPlan`.
- `crates/sqe-planner/src/rewrite/`: new module holding the rule; unit-tested
  in isolation against a synthetic `LogicalPlan` fixture.
- `crates/sqe-coordinator/tests/in_subquery_view_rewrite.rs`: rename + keep
  the behavioural assertions (NULL handling, NOT IN, empty subquery, multi-
  column tuple). The mechanism test (`scratch_registers_when_session_default_catalog_rejects_registration`)
  becomes obsolete once there is no scratch table to register.

**Acceptance:**

- `lift_in_subqueries` is gone from `write_handler.rs`; no scratch MemTable
  ever registered.
- TPC-E SF10 `trade_result_update_holding` completes in under the current
  10.94 s baseline.
- TPC-E SF100 `trade_result_update_holding` completes without bumping the 8 MiB
  stack (bundle this with follow-up #1 if possible).
- All 10 tests in `in_subquery_view_rewrite.rs` pass after being re-pointed at
  the new rewrite.
