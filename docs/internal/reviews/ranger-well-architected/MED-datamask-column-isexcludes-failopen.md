# Datamask column loop ignores column.isExcludes (fail-open complement)

- **ID:** datamask-column-isexcludes-failopen
- **Pillar:** Security
- **Severity:** Medium
- **Status:** Resolved in commit 722d1b2
- **Files:** crates/sqe-policy/src/ranger_store.rs:342-366

## Problem
Database and table matching honored `isExcludes` via `resource_matches`, but the datamask column loop read `col_res.values` directly and ignored `column.isExcludes`. A policy written as "mask all columns EXCEPT these" was silently treated as "mask ONLY these." Every column the operator intended to mask was left raw.

## Proposed fix
When `column.is_excludes` is set, fail closed: push a `lit(false)` deny and warn. The complement set requires a schema that the resource path does not have, so denying is the only safe interpretation.

## Acceptance criteria
A bundle with `column.isExcludes=true` denies (a `lit(false)` row filter), and no include-style masks are produced.
