**STATUS 2026-11-22: Databricks column tag syntax proven end to end (`test/databricks-column-tags-e2e-922`, #922).**

- What was already on main: `ALTER TABLE t ALTER COLUMN c SET TAGS ('k' = 'v', 'k2')` / `UNSET TAGS ('k')` parse (!1885) into the same `SetTagsStatement` / `ColumnTagOp` as `SET TAGS (c = (...))`, so they take the same tag write, guard and Ranger projection. `SHOW TAGS ON t` returns `column, tag, value, applied` (#923). The only gap was evidence: no e2e test used the Databricks form or asserted a SHOW TAGS value.
- Added to `access_control_e2e`: `databricks_column_tags_mask_project_and_show_values` (an admin tags `ssn` with a value and `email` key-only. The tag is projected into Ranger's tag store, the tag mask applies, SHOW TAGS shows `restricted` and `''`. `UNSET TAGS ('PII')` removes the tag, the projection and the mask) and `databricks_column_tags_take_the_tag_guard` (the default `admin-role` gate refuses a non-admin's SET and UNSET; under `ranger-delegate` a steward with `alter` on `ssn` only tags and untags `ssn`, and is refused on `email` with nothing written).
- Gate: `scripts/access-control-test.sh databricks_column_tags`, 2 passed, exit 0, on the Polaris + Ranger + Keycloak stack.
- Not changed: untag rights (#927). `ALTER VIEW ... SET TAGS` is not parsed and needs view tag storage.

**NEXT:** unchanged.
