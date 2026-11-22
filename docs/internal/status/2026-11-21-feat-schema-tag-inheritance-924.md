**STATUS 2026-11-21: schema tags inherited behind `policy.inherit_schema_tags` (`feat/schema-tag-inheritance-924`, #924, openspec tag-inheritance 2.0-2.4).**

With `[policy] inherit_schema_tags = true` a column's effective tags are its own, then its table's, then its schema's; the nearest key wins. The schema's `sqe.schema-tags` is read with the caller's token on every table load (no namespace cache), so a new or removed schema tag takes effect from the next statement, in every session. The tag state is three-valued (not inherited, known, unknown): a namespace that cannot be read or a malformed property makes the table's tag state unknown, which refuses the read, SHOW STATS and tag-aware writes. Every Iceberg provider constructor now takes the schema tag state, so a new call site cannot forget it. The cached tag source returns unknown with the flag on, and the DML policy probe reads through a real table provider instead of an in-memory copy.

The flag is off by default: enabling it widens what a schema tag masks in a released image, so it waits until the tools that manage schema tags expect that.

Gate: `make test-integration FILTER=schema_tag_inheritance` 4 passed; `FILTER=tag_` 44 passed with the flag off and with `SQE_POLICY__INHERIT_SCHEMA_TAGS=true`. Lib tests sqe-core, sqe-catalog, sqe-write, sqe-policy green; clippy and budgets clean.

Not covered by a test: a namespace the caller cannot read (403). It takes the same unknown path as the malformed property, which is tested. The Ranger tag projection still carries own tags only (#935), and catalog tags remain metadata (#924). The flag-on DML path costs one extra table load and one namespace load per statement.

**NEXT:** make `refuse_at_write_mask_conflict` and `tag_mask_beats_resource_mask_live` mode-aware (test MR); then catalog-tag inheritance (#924).
