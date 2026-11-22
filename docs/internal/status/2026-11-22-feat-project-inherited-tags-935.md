# feat/project-inherited-tags-935

**Date:** 2026-11-22
**Issue:** #935 part 1 (Ranger projection of inherited tags), for #924 / #930

Table tags (`sqe.table-tags`) are now projected into Ranger's tag store, so
Spark masks and filters with them:

- Two tag service resources per table: `{database, table, column: *}` for
  masks and `{database, table}` for tag row filters. Checked live: Kyuubi did
  not apply a tag on the bare table resource to columns (rows stayed raw for
  180 s); the `column: *` resource masked within seconds.
- Paths: `SET/UNSET TAGS` on a table (add before commit, remove after),
  `reproject_column_tags` (add-only), the tag load observer.
- Never shrink: a malformed `sqe.table-tags` in the observer logs an error and
  keeps the last projection; own column tags still project. Unit test
  `a_failed_parent_read_keeps_the_projected_table_tags`.
- Live e2e `spark_mask_parity_e2e::table_tag_masks_every_column_in_both_engines`.

Known gaps: Spark cannot refuse a table whose projection is stale; tags written
outside SQE project on SQE's next load of the table; no metric or audit event
for projection failures; schema tags not projected.

**NEXT:** #935 part 2: project schema tags (`{database, table: *, column: *}`
and `{database, table: *}`) when `inherit_schema_tags` is on.
