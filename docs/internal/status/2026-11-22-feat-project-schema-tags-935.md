# feat/project-schema-tags-935

**Date:** 2026-11-22
**Issue:** #935 part 2 (Ranger projection of schema tags), for #924 / #930

Schema tags (`sqe.schema-tags`) are projected into Ranger's tag store when
`inherit_schema_tags` is on:

- Resources `{database, table: *, column: *}` (masks) and
  `{database, table: *}` (tag row filters). Checked live with a probe:
  Kyuubi applies a MASK_NULL tag mask and a tag row filter on both.
- `ALTER SCHEMA SET|UNSET TAGS`: before the namespace commit Ranger gets
  held ∪ before ∪ after (failure refuses, nothing changed); after it the
  exact list, or a delete when empty (failure over-masks, logged).
- Load observer: reads the schema state with the table (one read), only
  grows the schema-wide resource (one replace of held ∪ new). `Unknown`
  logs an error and changes nothing; the table's own tags still project.
  Test `an_unreadable_schema_keeps_its_projection_and_the_table_still_projects`.
- `reproject_column_tags`: one `<schema>.*` row per schema, exact.
- Ranger import replaces (`add_or_update` = set the list, `delete` = drop
  the resource), so a non-empty table- or schema-level change is now one
  replace; delete then add left Spark unmasked in between.
- Live e2e `spark_mask_parity_e2e::schema_tags_mask_and_filter_every_table_in_both_engines`.

Known gaps: per-process memory of what is held (a second coordinator can
re-add a removed key: over-mask); flag off or DROP SCHEMA leaves the `*`
resources; a cold-memory reproject of a tagless schema does not delete a
stale `*` resource; no metric or audit line for projection failures; Spark
cannot refuse a stale projection; precedence of own/table/schema tags is #942.

**NEXT:** column-level projection in sqe-policy still deletes then adds;
send a non-empty column tag set as one replace, as the table and schema
levels now do.
