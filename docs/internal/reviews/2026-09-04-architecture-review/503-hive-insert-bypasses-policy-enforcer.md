# 503: INSERT into a Hive external table bypasses the policy enforcer on its source SELECT and authorizes the target after the write

- Scope: sqe-coordinator write path (query_handler Hive INSERT/CTAS arms, hive_write.rs, hive_ddl_handler.rs)
- Severity: high
- Category: security
- Effort: S (hours)
- Status: open

## Problem

Row filters, column masks and column restrictions are applied to reads by an
explicit call, not by a DataFusion analyzer rule:
`crates/sqe-coordinator/src/query_handler.rs:3481`
(`self.policy_enforcer.evaluate(&session.user, plan)`) on the SELECT path and
`WriteHandler::enforce_source_plan` on the Iceberg INSERT/CTAS path
(`crates/sqe-coordinator/src/write_handler.rs:1497-1498`). The `policy_store`
handed to the session context is only used for `information_schema` visibility
(`crates/sqe-catalog/src/catalog_provider.rs:243-259`,
`info_schema.rs:212`); table providers are not policy-wrapped.

The Hive INSERT arm skips that call entirely.
`crates/sqe-coordinator/src/query_handler.rs:1716-1724`:

```rust
if self.is_hive_table(session, &hive_ref) {
    let (ctx, _) = self.create_session_context(session).await?;
    let written = crate::hive_write::handle_insert(&ctx, stmt, &format!("{stmt}")).await?;
```

`crates/sqe-coordinator/src/hive_write.rs:102-109` plans the raw statement
text with `ctx.sql(sql)` and collects it. The Hive CTAS arm feeds the same
function (`query_handler.rs:1631-1656`, `ctas_load_sql` then `handle_insert`).

The target-side check runs only after the sink has written:
`refresh_index_after_write` -> `repair_index` -> `refuse_if_denied`
(`crates/sqe-coordinator/src/hive_ddl_handler.rs:572-593`). A denied caller
gets an error whose text says the rows WERE written and are recoverable with
`MSCK REPAIR TABLE` (`query_handler.rs:1739-1747`).

## Why it matters

`INSERT INTO lake.dump.t SELECT * FROM masked_iceberg_table` copies unmasked,
unfiltered rows from a policy-protected Iceberg table into a Hive directory
the caller then reads with whatever tool they like. This is the exfiltration
path the Iceberg INSERT path closed with `enforce_source_plan` (its comment at
`write_handler.rs:1494-1496` names the exact scenario). Two write paths, one
guarded, one not.

Separately, a caller denied on the Hive target still lands data files in the
bucket before the denial fires; the index refresh is refused, but the objects
exist and a later `MSCK REPAIR TABLE` by anyone makes them visible.

## Proposed change

1. In `hive_write::handle_insert`, plan the statement, take the source plan
   (`Dml::Insert.input`), run it through the same `PolicyEnforcer` the Iceberg
   path uses, rebuild the INSERT plan with the enforced input, and execute
   that. Signature gains `&dyn PolicyEnforcer` and `&Session`; the
   `WriteHandler` already holds the enforcer, so route Hive inserts through it.
2. Call `HiveDdlHandler::refuse_if_denied` on the target manifest before
   planning the write, in both the INSERT and CTAS arms. Keep the post-write
   check as belt and braces.
3. Record the policy summary into the audit entry like the Iceberg arm does
   (`policy_summary_out`).

## Acceptance criteria

- Unit test with `InMemoryPolicyStore` + `PolicyPlanRewriter` (pattern at
  `write_handler.rs:8383-8421`): a mask on `salary` results in masked values in
  the parquet written to a Hive fixture (`tests/hive_write_test.rs` has the
  fixture and read-back helper).
- Test: a user fully denied on the Hive target gets an error and no object
  appears under the table LOCATION.
- Test: Hive CTAS from a row-filtered table contains only admitted rows.

## Related

- `docs/internal/reviews/2026-07-25-production-readiness-review.md` section 4
  ("policy rewriter is deliberately fail-closed") assumed a single enforcement
  point; this path predates it.
- Memory note "Hive external tables: cycle 3 (tag masks) unstarted".
