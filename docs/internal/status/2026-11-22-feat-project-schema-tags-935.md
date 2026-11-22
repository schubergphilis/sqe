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

Review follow-ups, fixed in this MR rather than left open:

- Ranger, not process memory, decides what is held. `ALTER SCHEMA` and
  `reproject_column_tags` read it (`GET /service/tags/secure/download/{service}`,
  filtered client-side: Ranger 2.8 ignores the resource filters on its
  tag endpoints); a failed read refuses `ALTER SCHEMA` and writes nothing.
  The load observer trusts its schema memory for 120 s, then reads Ranger
  once for all loads queued on the schema; an unreadable Ranger writes
  nothing. Table-level memory older than 300 s is resent as a replace.
  Rig test `a_set_after_another_coordinators_unset_writes_ranger` (two
  coordinators, one fake Ranger).
- `DROP SCHEMA` deletes the schema-wide resources; `reproject_column_tags`
  with the setting off, or on a tagless schema, deletes what Ranger holds
  (cold memory included). Rig test `drop_schema_clears_the_schema_wide_resource`.
- Failures are counted: `sqe_tag_projection_failures_total{op=write|read|source}`.
- Column-level projection is one replace per changed column, delete only
  for a column left with no tags (folded in: same file and trait).
- Live e2e deletes the schema-wide resource out of band and checks that
  the next `ALTER SCHEMA SET TAGS` puts it back and Spark masks again.

Limits that remain (by design, stated in D5 and Risks): a removed key can
come back from another coordinator's memory for at most the TTL (over-mask
only); each read downloads the whole tag store of the service; without
`catalog_scoped`, same-named schemas in two catalogs share one resource;
Spark cannot refuse a stale projection; precedence of own/table/schema
tags is #942 (undecided); catalog tags stay refused pending the storage
decision, so #930 stays open.

**NEXT:** #942 precedence decision (owner), then catalog-tag storage
decision before any catalog-level projection.
