# apply_tag_ops clones the whole tag map (DDL path only)

- **ID:** apply-tag-ops-map-clone
- **Pillar:** Performance
- **Severity:** Low
- **Status:** Open
- **Files:** crates/sqe-coordinator/src/tag_source_impl.rs:141

## Problem
`apply_tag_ops` clones the whole tag map. It runs on the DDL path over a handful of columns, NOT the query/scan/row path, so there is no query impact.

## Proposed fix
Optional: take the current map by value and mutate in place.

## Acceptance criteria
n/a.
