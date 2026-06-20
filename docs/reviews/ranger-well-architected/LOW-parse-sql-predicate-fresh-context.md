# parse_sql_predicate builds a fresh SessionContext per call in the tag path

- **ID:** parse-sql-predicate-fresh-context
- **Pillar:** Performance
- **Severity:** Low
- **Status:** Open
- **Files:** crates/sqe-policy/src/policy_expr.rs:93-99; callers crates/sqe-policy/src/ranger_store.rs:282,376,517; crates/sqe-policy/src/plan_rewriter.rs:383

## Problem
Each parse builds a fresh `SessionContext` and registers 5 UDFs. This is acceptable at `resolve()` cache-miss frequency, but in the tag path it repeats per query (because `resolve_tags` is uncached, see `resolve-tags-no-bundle-cache`) and per CUSTOM-template column.

## Proposed fix
Largely subsumed by the bundle cache. Additionally, parse once per `(tag, column)` and reuse, or use a shared lazily-built `SessionContext` per resolution pass.

## Acceptance criteria
`SessionContext::new()` calls per tagged-join query drop to O(distinct tag exprs).
