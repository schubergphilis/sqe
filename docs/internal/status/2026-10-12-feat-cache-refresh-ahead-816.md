**STATUS 2026-10-12: Ranger bundle refresh-ahead and single-flight; jittered table-metadata soft TTL (`feat/cache-refresh-ahead-816`, #816 part 1).**

Ranger policy bundle (`ranger_store.rs`):
- Concurrent misses now wait on one download instead of each downloading the full bundle.
- From 80% of `cache_ttl_secs`, the one reader that wins a `try_lock` downloads a fresh copy; every other reader keeps the cached one.
- A failed refresh-ahead serves the cached bundle until its TTL, then queries fail closed as before.
- A download in flight when `invalidate_all` runs (a GRANT through SQE) is not cached, so it cannot put a pre-GRANT bundle back. Before this, a cold miss racing a GRANT could do that too.

Table metadata cache (`rest_catalog.rs`): each key's soft TTL is shortened by 0-20%, fixed per key by a hash, so tables loaded together do not revalidate together. The jitter never lengthens it: the policy-tag path reads only fresh entries (architecture review 303). Metadata has no refresh-ahead: the cache holds no fetcher, the stale path already revalidates with a conditional GET, and the entry's vended-credential expiry would complicate a background reload.

Tests: four wiremock tests for the bundle (cold misses download once, an ageing bundle refreshes once, a failed refresh serves the cached bundle, a download racing `invalidate_all` is not cached). One test shows that 1,000 entries cached at one instant spread their expiry over the last 20% of the TTL.

Not done here: the namespace name listing (`needs_refresh` against a fixed `TABLE_NAMES_TTL_SECS`) has the same same-moment expiry. #816 part 2, the audit and lineage sink chaos tests, is a separate branch.

**NEXT:** #816 part 2 (sink chaos tests), then the `{catalog}` placeholder for `[storage.tvf]` staging prefixes.
