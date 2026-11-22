# fix/tag-projection-readback-935

**Date:** 2026-11-22
**Issue:** #935 follow-up (gaps of !1936, schema-tag projection into Ranger)

Fixes the known gaps !1936 listed, instead of leaving them open:

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
