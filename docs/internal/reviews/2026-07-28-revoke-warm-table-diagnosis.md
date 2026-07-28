# REVOKE does not take effect for an already-read table: code-level diagnosis

Item 7 of the ACL handoff plan
(`data-platform/docs/superpowers/plans/2026-07-28-sqlengine-acl-handoff-prompt.md`).
Status: **diagnosis in progress, no fix committed.** This file records what the code
says, so the next session does not redo it.

## The defect

After a revoke, Ranger is verifiably clean (`chameleon access show ...` returns 0
policies) yet a table that had been read once keeps returning rows. Measured on the
quickstart with shipped config, no restarts:

```
grant  +15s -> allow            (table now warm)
revoke +2s .. +251s -> allow    (17 consecutive reads)
```

A table that was never read is denied throughout. `docker compose restart sqe`
denies immediately. So the retained state is process-local, per-table, and
populated by the first successful read.

## Config context that changes how the eliminations read

From `data-platform/quickstart/sqe/assets/sqe-config/sqe.toml`:

- `[policy] engine = "passthrough"` -- **no `PolicyStore` is wired.**
- `metadata_cache_ttl_secs = 30`, set in **both** `[catalog]` and
  `[catalogs.main_warehouse]`.
- `[query_cache] enabled = true`, `ttl_secs = 300`.
- `[storage] s3_access_key`/`s3_secret_key` present (SQE reads data files itself).

## Findings

### 1. The policy cache is not a candidate here, and cannot be

`invalidate_policy_cache` (`query_handler.rs:4455`) is a no-op when no
`PolicyStore` is wired, and the quickstart runs `engine = "passthrough"`. The moka
policy cache in `ranger_store.rs:172` (TTL from `cache_ttl_secs`, default **60s**,
`crates/sqe-core/src/config.rs:2728`) is never constructed on this path.

This matters because the plan's closing note ("`api_catalogs_refresh` does not call
`invalidate_policy_cache()`") reads like a likely cause. It is a real gap worth
closing for the day OPA/Ranger enforcement is switched on, but it **cannot** explain
the observed window in a passthrough deployment. Do not spend time there first.

### 2. `TableMetadataCache` has a 1-hour HARD TTL; `metadata_cache_ttl_secs` is only the SOFT TTL

`rest_catalog.rs:157-179`. `metadata_cache_ttl_secs` sets `soft_ttl`; the moka
`time_to_live` is hardcoded to **3600s** so ETag revalidation still has an entry to
revalidate against.

A 1-hour hard TTL fits the observations that no 30s/60s/300s TTL does: a window
longer than 251s, bounded rather than permanent, and flipping later in the same
session.

Caveats that keep this from being confirmed:

- `ttl_secs = 0` takes the `max_capacity(0)` branch, so the cache genuinely stores
  nothing. The plan's `metadata_cache_ttl_secs = 0` test would therefore be a valid
  elimination **if** the value reached this constructor.
- The constructor reads `config.catalog.metadata_cache_ttl_secs`
  (`bin/sqe_server.rs:1109`) -- the top-level `[catalog]` block, **not**
  `[catalogs.main_warehouse]`. The quickstart sets both. **Verify which block the
  0 was written into.** If it went only into `[catalogs.main_warehouse]`, the
  elimination is void and this is the leading candidate.
- There is exactly one production instance (`bin/sqe_server.rs:1109`), shared into
  both `HealthState` and the read path, and `api_catalogs_refresh` does invalidate it
  (test `api_catalogs_refresh_invalidates_table_cache`). So the refresh endpoint hits
  the right instance -- this is NOT the wrong-instance bug seen elsewhere.

### 3. The 304 revalidation path is the specific mechanism to test

`rest_catalog.rs:1140-1220`. On a stale entry SQE sends `loadTable` with
`If-None-Match`. On **304** it calls `table_cache.revalidate(&cache_key)` -- which
resets `validated_at` (`rest_catalog.rs:213-216`) -- and returns the stale table.

So SQE treats "metadata unchanged" as "still authorized". Whether that is exploitable
depends on Polaris: **if Polaris evaluates the ETag short-circuit before the Ranger
authorization check, a revoked user's revalidation returns 304 forever** (the
metadata genuinely has not changed), each 304 refreshes the soft TTL, and access
persists until the 1-hour hard eviction. That reproduces every observation including
the Polaris-restart elimination, since Polaris would still answer 304 after a
restart.

A 403 on that request is handled correctly (falls through to the full load, which
denies), so the bug only exists on the 304 branch.

**Decisive experiment, no rebuild needed:** as a revoked user, call Polaris
`GET .../namespaces/<ns>/tables/<t>` directly with `If-None-Match: <etag>` and see
whether it answers 304 or 403. If 304, this is the root cause and the fix is in SQE:
a 304 must not be trusted as an authorization result. Options are to drop the
conditional-request path for authorization-sensitive loads, or to bound the hard TTL
to something defensible and treat the soft TTL as the real security window.

### 4. The query result cache is user-scoped, so it is not a cross-user leak

`query_cache.rs:99` keys on `sha256("{user}:{whitespace-normalized-uppercased-sql}")`,
TTL 300s in the quickstart. Novel SQL text is a genuine miss, so the plan's
vary-the-text test is a valid elimination **provided the varied text had never been
run during the allowed phase** (any text run while access was still granted is cached
under its own key and would keep answering until its own 300s expiry). The 300s TTL
is close enough to the observed 251s that this is worth restating precisely rather
than assuming.

Note that nothing on the grant/revoke path clears the result cache. Independent of
the root cause, a revocation should invalidate cached results for the affected
resource, or cached rows outlive the grant that authorized them by up to the cache
TTL.

## Ranked next actions

1. Confirm which config block carried `metadata_cache_ttl_secs = 0` in the original
   test. This single fact decides whether finding 2 is live.
2. Run the 304-versus-403 probe against Polaris (finding 3). No rebuild.
3. Measure to convergence with the result cache disabled and a never-before-run
   query, to get a clean N.
4. Independently of the above, close the two gaps that are wrong on their own terms:
   `api_catalogs_refresh` should invalidate the policy cache the way
   `handle_grant`/`handle_revoke` do (so externally-issued grants behave like SQL
   ones), and revocation should drop affected cached results.

Do **not** raise `RANGER_SETTLE` to make the platform e2e green.
`data-platform/quickstart/sqe/scripts/test-user-group-grant-access-e2e.sh` step 8 is
red on purpose.
