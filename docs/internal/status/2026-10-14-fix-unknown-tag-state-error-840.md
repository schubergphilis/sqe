**STATUS 2026-10-14: an unknown tag state refuses the statement with `AccessDenied` instead of returning 0 rows (`fix/unknown-tag-state-error-840`, #840).**

`PolicyPlanRewriter::evaluate` (`plan_rewriter.rs`) used to inject `lit(false)` when `TagSource::column_tags` returned `None`. The query succeeded with 0 rows and a CTAS created an empty table, which is how the file-TVF over-deny fixed in !1413 stayed hidden. It now returns `AccessDenied` naming the table and "tag state unknown". It still fails closed. The file-scan and reserved-virtual-ref exemptions are unchanged.

Scope: only the tag-unknown case. The two other silent denies were left as row filters: the un-inlined view scan and the unresolvable policy key.

The "cold handler needs up to 60 s" note in `tests/it/common/mod.rs` does not describe production. `SESSION_CONTEXT_CACHE` is process-global, so a second handler in the same test process reuses the first handler's cached session. Its table loads fill the first handler's `TableMetadataCache`, not the one its `CacheTagSource` reads. A coordinator has one handler and one cache, and the planning-time `load_table` fills it (`rest_catalog.rs`, `insert_if_generation`).

Tests:
- `sqe-policy` passes: 337 unit and 88 integration tests.
- The `sqe-coordinator` lib passes 848 tests with `RUST_MIN_STACK=32MiB`.
- Updated: `unknown_tag_state_refuses_the_statement` (formerly `unknown_tag_state_denies_all_rows`), `file_table_scan_is_not_denied_for_unknown_tag_state`, and the Ranger e2e `unknown_tag_state_denies`.
- Live Ranger e2e (`scripts/access-control-test.sh`): `unknown_tag_state_denies` and `file_table_function_is_not_denied_for_unknown_tag_state` pass.
- Book: the three fail-closed tables now say "refuse the statement".

Production paths that still reach an unknown tag state, now an error where they returned 0 rows:
- `catalog.metadata_cache_ttl_secs = 0` (a capacity-0 cache: every governed query);
- the cache's 1000-entry cap (keys are per token and table, so eviction or moka refusing admission on a busy coordinator);
- a commit racing the planning load (`insert_if_generation` returns false).

Restarts and stale entries are not among them: planning goes through `SqeSchemaProvider::table` -> `load_table` (`schema_provider.rs:469`), which refreshes the entry.

**NEXT:** a tag-source fallback that loads table properties on a miss, so these paths get real tags instead of an error.
