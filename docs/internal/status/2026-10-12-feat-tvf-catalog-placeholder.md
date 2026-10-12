**STATUS 2026-10-12: `{catalog}` placeholder in `[storage.tvf] allowed_object_store_prefixes` (`feat/tvf-catalog-placeholder`).**

`"s3://staging/ingest/ws/{catalog}/"` expands to the session's catalog only when that catalog answered the caller's own bearer while the session was built: the same proof `tvf_prefixes` already needed. `TvfCaller` carries the proven catalog in its own field (`catalog_with_access`), separate from `scope_catalog`, which any session sets by naming a catalog. A discovered (`polaris-auto`) warehouse now joins the access set when its listing succeeds; the discovery template is per-user only.

Details:
- Without a proven catalog the entry is dropped, never matched as an empty segment (`.../ws//` stays closed).
- Names must be `[A-Za-z0-9._@+-]`, not only dots, without `..`. Matching is exact and case-sensitive.
- Combines with `{user}`; works in `[catalogs.<name>] tvf_prefixes` too. Hive callers never carry a catalog, so `{catalog}` entries never apply to Hive paths.
- Inline credentials and `allow_all_object_stores` still bypass. The docs now say a deployment relying on the prefixes needs `allow_inline_credentials = false`.
- The denial still names `allowed_object_store_prefixes`.
- "No session catalog" means a caller with no scope. A session that names none is scoped to the config default and expands `{catalog}` to it when proven, as `tvf_prefixes` already did.
- The proof is captured at session-context build, so a revoked grant holds until the cached context expires, as for `tvf_prefixes`.

Tests: sqe-core unit tests (proven / unproven / unscoped, unsafe names, combined with `{user}`, bypasses), the session_context access-proof test, and the live IT `tvf_catalog_placeholder_e2e`. In that IT, root reads its own workspace path in `discovery_test_wh` (discovered) and `test_warehouse` (config) and is refused across them; a fresh principal without grants is refused in both while naming them.

Found on the way: `read_parquet` builds its own S3 store and enables plain HTTP only with `s3_allow_http`. The other file readers also derive it from an `http://` endpoint, so `read_parquet` against an `http://` endpoint fails with a bare "builder error" (the IT sets `s3_allow_http`).

**NEXT:** fix `read_parquet` allow_http derivation; #835; #803 when it recurs.
