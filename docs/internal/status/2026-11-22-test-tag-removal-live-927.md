# test/tag-removal-live-927

**Date:** 2026-11-22
**Issue:** #927 (tag removal needs a stronger right than tagging)

The `tag_removal` setting (`same | remove_tag | admin`) and the `remove_tag`
Ranger access type shipped in !1907. This branch adds the evidence the issue
still asked for:

- Coordinator test: a policy store that cannot answer for `remove_tag` refuses
  every untag (column, Databricks column form, table) and lets every add through.
- Live Ranger (`make test-access-control`, filter `tag_removal`):
  - `remove_tag` mode: `apply_tag` on ssn tags it, but the three untag forms are
    refused, audited as `ACCESS_DENIED`, and the mask stays. A value change on
    the kept key passes. An allow item naming `remove_tag` lets the untag through.
  - `admin` mode: the steward's untag is refused and an admin's passes.
- OpenSpec `tag-removal-authority`: scenarios for the unresolvable right and the
  Databricks column syntax; tasks 4.3 and 4.4.

**NEXT:** an explicit select/USE check on the catalog and schema for tag writers
(the remaining part of the GitHub sqe#19 ask; today Polaris refusing the load stands in).
