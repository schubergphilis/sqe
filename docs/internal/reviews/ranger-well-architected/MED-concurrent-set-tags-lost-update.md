# Concurrent SET TAGS silently lose updates (last-writer-wins)

- **ID:** concurrent-set-tags-lost-update
- **Pillar:** Reliability
- **Severity:** Medium
- **Status:** Open
- **Files:** crates/sqe-coordinator/src/catalog_ops.rs:900-935,739; crates/sqe-catalog/src/rest_catalog.rs:1560-1626

## Problem
`set_column_tags` reads `sqe.column-tags`, merges, and commits `SetProperties` with `requirements vec![]`. Two concurrent `SET TAGS` read the same base map. The second commit overwrites the first (last-writer-wins), silently dropping a tag change.

This is NOT fixable with an Iceberg `TableRequirement`: there is no property-CAS variant, and a pure `SetProperties` bumps no checkable assertion, so Polaris returns no 409.

## Proposed fix
Serialize the read-merge-commit per `TableIdent` on the coordinator using an async mutex. Tag authoring is rare, so the contention cost is negligible. Do not propose a `TableRequirement`; it cannot work here.

## Acceptance criteria
Two concurrent `SET TAGS` on different columns of one table both survive, or one fails loudly. Neither is silently lost.
