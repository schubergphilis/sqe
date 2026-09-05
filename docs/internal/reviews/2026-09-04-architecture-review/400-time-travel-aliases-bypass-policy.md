# 400: Time-travel, version and incremental reads register under `datafusion.public.*` and bypass the policy rewriter

- Scope: sqe-coordinator, read side (query_handler.rs) with sqe-policy plan_rewriter.rs as the consumer
- Severity: high
- Category: security
- Effort: M (days)
- Status: fixed (branch `fix/authz-policy-enforcement-p1`, 2026-09-05)
- Issue: #580

## Problem

`FOR SYSTEM_TIME AS OF`, `FOR VERSION AS OF` and `FOR INCREMENTAL BETWEEN` are pre-processed
before planning. Each one loads the Iceberg table, builds a snapshot-pinned provider, registers it
under a fresh alias in the `datafusion.public` schema, and rewrites the SQL to point at that alias:

- `crates/sqe-coordinator/src/query_handler.rs:5251-5252`
  ```rust
  let alias = format!("__sqe_tt_{}_{}", bare_table, uuid::Uuid::now_v7().simple());
  let qualified = format!("datafusion.public.{alias}");
  ```
- `crates/sqe-coordinator/src/query_handler.rs:5159` (version spec) and `:5393-5397` (incremental) do the same with `__sqe_incr_` aliases.

The policy enforcer runs afterwards on the rewritten logical plan
(`query_handler.rs:3474-3481`, `:3120-3126`). It derives the policy key from the
`TableScan`'s `TableReference`, not from the provider:

- `crates/sqe-policy/src/plan_rewriter.rs:829-853` `resolve_policy_key` returns
  `(schema, table)`, so for the alias it yields `("public", "__sqe_tt_orders_<uuid>")`.
  No resource policy exists for that name; the store test
  `resolve_effective_policy_no_policy_is_empty_passthrough` (`plan_rewriter.rs:1961`)
  documents that no policy means passthrough.
- `crates/sqe-policy/src/plan_rewriter.rs:740-746` `is_reserved_virtual_ref` returns
  `true` for any reference whose catalog is `datafusion`, and the caller
  (`plan_rewriter.rs:233-243`) then treats the table as "known-no-tags", skipping
  the tag-mask lookup and its fail-closed branch.

There is no admin gate on these clauses (`query_handler.rs:4936-5290` contains no
`require_admin`, `authorize` or policy call), and no test in `tests/it/` exercises a
row filter or column mask together with `SYSTEM_TIME` or `FOR INCREMENTAL`
(`grep -rn SYSTEM_TIME tests/it/access_control_e2e.rs` returns nothing).

## Why it matters

A user with a row filter or column mask on `sales.orders` reads the unmasked, unfiltered
table by appending `FOR SYSTEM_TIME AS OF now()` or `FOR VERSION AS OF <current snapshot>`.
The Iceberg `load_table` still requires Polaris to grant table metadata, so a user with no
table access at all is still refused, but every fine-grained control SQE adds on top of
Polaris (row filters, masks, tag policies, DENY on columns) is inert on these paths. The
audit entry records the alias, not the real table, so the read is also invisible to SIEM
rules keyed on the table name.

## Proposed change

Keep the table identity on the plan, not in the name. Options in order of preference:

1. Register the pinned provider under the real `catalog.namespace.table` reference inside a
   per-query `SessionContext` clone (DataFusion supports `SessionContext::new_with_state` from
   a cloned `SessionState`) instead of a shared context plus alias. The rewriter then sees the
   real reference and everything downstream (policy, audit, lineage, cache invalidation) works
   unchanged.
2. If the shared-context design must stay, teach `PolicyPlanRewriter` to unwrap the
   `SqeTableProvider` / `IncrementalTableProvider` from the `TableScan` source and read the
   real `TableIdent` from it, and remove the `datafusion` catalog from
   `is_reserved_virtual_ref` for those providers.

Either way, add the real table name to `audit_resources::resources_from_plan` for these scans.

## Acceptance criteria

- New `tests/it/access_control_e2e.rs` cases: a user with a row filter and a column mask on a
  table gets identical results from `SELECT * FROM t`, `... FOR SYSTEM_TIME AS OF <now>`,
  `... FOR VERSION AS OF <current>` and `... FOR INCREMENTAL BETWEEN ...`.
- A user DENIED a column gets the same denial on all four forms.
- The audit event for a time-travel read names `catalog.ns.table`, not `__sqe_tt_*`.
- `cargo test -p sqe-policy` still passes the `is_reserved_virtual_ref` information_schema cases.

## Related

- Prior review 2026-07-25 P1-7 (metadata paths turn into empty results) is adjacent but did
  not cover this bypass; nothing in the 2026-06-26 review covers it.
- Memory note "RENAME/DROP carry their tags" shows the same class of bug (identity lost on a
  rewrite) was fixed for DDL in !794.

## Resolution (2026-09-05)

`TimeTravelCleanup::restore_identities` (`crates/sqe-coordinator/src/query_handler.rs`) rewrites every `datafusion.public.__sqe_*` pinned scan back to the user's `TableReference` (wrapped in a `SubqueryAlias` so the plan above keeps resolving) before the plan reaches the policy rewriter and the audit. `handle_time_travel`, `handle_incremental` and `process_time_travel_table_factor` record the alias-to-identity pairs, and both planning sites (explain and execute) apply the restore. Test `restore_identities_renames_pinned_scans_and_keeps_the_plan_valid` registers a pinned provider, restores it and checks the scan carries the user's reference and the plan still executes.
