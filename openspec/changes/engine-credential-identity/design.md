## Context

Probed on the test stack (Polaris 1.9, RustFS STS), 2026-11-22:

| Principal | plain loadTable | delegated loadTable | `/credentials` | GET own table | GET sibling table | PUT own table |
|---|---|---|---|---|---|---|
| metadata only | 200 | 403 | 403 | - | - | - |
| + `TABLE_READ_DATA` | 200 | 200 | 200, one prefix = table location | ok | AccessDenied | AccessDenied |
| + `TABLE_WRITE_DATA` | 200 | 200 | 200 | ok | AccessDenied | ok |

- `/credentials` keys carry the caller's privilege and are scoped to one table. An engine with write-data would get write keys for every table a person can load.
- A plain load's `config` holds `s3.endpoint` and `s3.path-style-access` only; `/credentials` returns the four key entries (`s3.access-key-id`, `s3.secret-access-key`, `s3.session-token`, `s3.session-token-expires-at-ms`). Endpoint and path style keep coming from the load config and `[storage]`.
- With `require_vended_credentials` on, FileIO gets no `[storage]` keys and config loading is disabled (`insert_s3_vended_isolation_props`), so a table without vended keys fails on storage; it cannot fall back to the coordinator's identity.
- The DML pre-check (#880, !1729) relies on the person's keys: Polaris lets a principal with `TABLE_WRITE_PROPERTIES` commit a snapshot, and only the storage probe with the person's read-only keys stops it. Engine keys would blind that probe.
- Ranger `select` is default-deny for registered Hive roots only (`RangerStore::requires_access_grant`). An Iceberg table with no Ranger access policy resolves unrestricted, so a gate built on `policy_denies_all` alone would pass every metadata-only person.

## Decisions

**D1. Engine keys are for reads only, enforced by a marker.** A table filled with engine keys carries the FileIO property `sqe.key-source = engine`. The writer and the DML storage probe refuse a table with that marker, so a write path that forgot to load through the delegated client fails instead of writing with engine keys. Every write path (INSERT, UPDATE, DELETE, MERGE, TRUNCATE, CTAS / `create_table`, commits) loads the target through `load_table_for_write`: the person's delegated client, unchanged behaviour. The session table cache is split by purpose (read / write) so a read-loaded table is never handed to a writer. Metadata-only people therefore cannot write; that is the intended authority (Polaris write-data), stated in the docs.

**D2. Config.** `CatalogConfig.credentials: Option<CatalogAuthConfig>` (only `client_credentials` accepted), separate from `auth`, which replaces the person's bearer. The engine token comes from `sqe_auth::per_catalog` (`resolve_bearer` with no session bearer), cached per (endpoint, client_id, scope).

**D3. One point of fetch.** `SessionCatalog::load_table_inner` and `load_table_uncached` (the reloader) are the only places engine keys are fetched. With the identity set, the read client is built without the delegation header (`access_delegation_props` empty); a second, delegated client serves `load_table_for_write` and `create_table`.

**D4. Select gate.**
- `SessionCatalog` takes an `Arc<dyn ReadGate>` (`async fn may_read(&self, table: &TableIdent) -> SqeResult<bool>`). With the identity set and no gate, the load errors and no keys are fetched.
- The coordinator builds the gate from the policy store with the same `TableScope` the plan rewriter uses, on `resolve_effective_policy` + `policy_denies_all`.
- **Default deny:** `RangerStore` gets the set of catalogs with an engine identity. Under `catalog_scoped = true` (required, D9) a namespace key starts with its catalog name, so a namespace of such a catalog requires an access grant like a Hive root (`requires_access_grant`). Other catalogs are unchanged.
- **Denied table:** returned with schema, no keys, and the FileIO property `sqe.read-denied = true`. The rewriter's `lit(false)` gives zero rows; for scans it does not prune (CASE WHEN guard, statistics-answered `count(*)`), `TableProvider::scan` returns an empty plan for a marked table before `plan_files`, so no manifest is read. Metadata tables, `SHOW STATS` and Puffin sidecar reads check the marker too. A table without keys and without the marker still fails on storage: only the marker empties a scan, so a missing key never becomes a silent zero.
- A marked table is not cached: the next statement runs the gate again, so a new grant takes effect at the next policy refresh.

**D5. Fetch.** A raw GET on `{rest_prefix}/namespaces/{ns}/tables/{t}/credentials` (the existing percent-encoding URL builder, shared `reqwest::Client`, SigV4 signer when configured) with the engine bearer. Pick the storage credential whose prefix is the longest prefix of the **person-loaded** metadata location; none -> error. Merge with the SQE-patched `table_file_io_props` precedence (made `pub` under the existing SQE PATCH marker), not a copy. Only the key entries are taken; nothing else from the engine response is used.

**D6. Failure.** A `/credentials` failure (403, 5xx, no match, token error) fails the statement. No fallback to the person's delegation header.

**D7. No sharing.** A table carrying engine keys is cached only in the person's session catalog, never in a cross-session cache keyed on the engine. Keys are held in `VendedKeys` with a hand-written redacting `Debug`.

**D8. Audit.** `sqe-catalog` cannot reach the `AuditLogger`; it takes an optional callback wired by the coordinator, which writes `AuditKind::Auth` with the person's `Actor`, `app_name = engine client_id`, the table as resource, outcome success or `error_type = "engine_credentials"`. Same shape as `catalog_scope_gate::refuse`.

**D9. Startup.** The server refuses a `credentials` block whose mode is not `client_credentials`, on a catalog with `require_vended_credentials = false`, when the policy backend is not Ranger (the only backend with default-deny `select`), or when `[policy.ranger] catalog_scoped` is off (bare keys cannot tell catalogs apart, so per-catalog default deny is impossible). The in-memory store is accepted only in test builds (its tests add explicit deny policies).

## Risks

- An engine principal with write-data is not exploitable through SQE (D1), but its keys reach workers in tickets; the docs recommend a read-only engine principal.
- Default deny hides every table of an identity catalog that has no Ranger access policy; operators must add `select` policies before enabling the identity. The docs say so. The identity is off by default.
