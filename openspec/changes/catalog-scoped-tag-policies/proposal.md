# Tag policies scoped to a catalog (#919)

## Why

Under `[policy.ranger] catalog_scoped`, resource policies name `<catalog>.<namespace>` and govern one catalog. Tag policies did not: a policy on tag `pii` applied to every column tagged `pii` in every catalog. Platforms that give each workspace its own catalog want each workspace's `pii` policy to govern only that workspace's tables, the way a Unity Catalog policy attached to a catalog governs that catalog's securables.

## What Changes

- With `catalog_scoped` on, a column of catalog `c` tagged `pii` is resolved against two tag policy names: `pii` and `c.pii`.
  - A policy on `c.pii` (`CREATE POLICY ... ON TAG c.pii`) governs catalog `c` only.
  - A policy on bare `pii` keeps applying in every catalog, so turning the setting on never unmasks a column.
  - The catalog's mask wins over the bare one. Row filters from both apply. A tag unmappable under either name restricts the column.
- Column tags stay bare in `sqe.column-tags`: a tag's catalog is its table's catalog.
- The Ranger tag projector writes both names on the column, so Spark applies the same policies.
- The `table_files` / `table_partitions` metadata functions key their tag and policy lookups on the catalog they read, not always on the session's.
- Policies written in SQL follow the session's catalog: `CREATE POLICY ... ON TAG pii` stores the policy on `<catalog>.pii`, `SET MASKING POLICY` keeps the column tag bare, and `CREATE OR REPLACE POLICY` refuses to move a policy to another tag.
- A bare tag policy from before the setting keeps applying in every catalog. That is accepted only because it adds masks and never removes one; the one exception (a column literally tagged `c.pii` in another catalog) is #943.
- With `catalog_scoped` off, nothing changes.

## Why a qualified tag value, not a resource

Ranger's `tag` service definition has one resource, `tag`. A catalog resource would need a service definition change that Spark's plugin does not know, and a policy label is not evaluated by Spark. A qualified tag value works in both engines with no Ranger change.

## Impact

- `crates/sqe-policy/src/tag_scope.rs` (new), `plan_rewriter.rs`, `tag_projector.rs`
- `crates/sqe-catalog/src/iceberg_metadata_tvf.rs`
- Coordinator callers of `resolve_effective_policy`
- Book: fine-grained access control, "Which `database` a policy names"
