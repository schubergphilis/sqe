**STATUS 2026-11-16: a tag committed by another engine masks the next query (`feat/tag-refresh-on-load-903`, part of #903).**

Before: a table's `sqe.column-tags` came from the shared metadata cache, so a tag committed out of band (Spark, another replica) left the column readable for up to `metadata_cache_ttl_secs`.

Now, under any policy engine, the first load of a table in a statement goes through the existing conditional-GET branch of `load_table_inner`:

- A 304 refreshes the soft TTL and the authorization window, then serves the cache.
- A changed table is reloaded.
- Later loads in the same statement serve the cache. The once-per-statement marker lives in the statement task-local (`file_tvf_prepare::first_check_in_statement`), not in the session catalog, which is cached per token across statements.
- Outside a statement scope, every load checks.
- Non-REST backends are skipped.
- `properties_for` now takes the most recently validated fresh entry, not the first match.

Knob: `[catalog] revalidate_each_statement`, which defaults to on unless the policy engine is passthrough. The cost is one catalog round trip per table per statement, no longer skipped inside `authorization_recheck_secs`.

Tests:

- `access_control_e2e::tag_from_out_of_band_commit_masks_the_next_query` commits the tag with a raw iceberg transaction under a 300 s cache TTL.
  - On unchanged code it failed: bob saw the plaintext `ssn`.
  - With the change it passes: `scripts/access-control-test.sh tag_from_out_of_band_commit`.
- Unit tests:
  - `a_table_is_checked_once_per_statement`
  - `table_metadata_cache_properties_prefer_the_newest_entry`
  - `statement_revalidation_defaults_on_when_a_policy_engine_enforces`

Left open on #903: ask 2, projecting a newly loaded tag into Ranger, so that Ranger-side tag policies see it without a SQE `ALTER`.

**NEXT:** #903 ask 2 (project tags to Ranger on load) once the catalog-scoped tag key MR lands.
