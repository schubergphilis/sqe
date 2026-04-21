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
