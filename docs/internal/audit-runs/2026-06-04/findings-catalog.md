# Findings — Catalog, TVFs & Cloud Catalogs (`sqe-catalog`)

**Scope:** Full `crates/sqe-catalog/src/`, priority on file/object TVFs and their SSRF/path-traversal
guard, credential vending/refresh, and cloud catalog backends. Verified the brief's lead on commits
`0ee68c0`/`03b010c`: they touch only `sqe-cli/src/embedded.rs` and correctly scope `allow_local_paths = true`
to the in-process CLI. The server `TvfPolicy` default stays fail-closed; coordinator/worker pass
`config.storage` (`allow_local_paths = false`); the `read_*` TVFs still call `tvf.check`/`tvf.check_endpoint`.
**The !190 guard itself is intact.** The real exposure is a parallel resolution path (CAT-01). The 3 `unsafe`
blocks are all `set_var` in `tests/`, not reachable from query input.

> **CAT-01 independently verified by the dispatcher** against `session_context.rs:232` and
> `lazy_object_store.rs:88-106`.

---

### CAT-01 — critical — Coordinator `enable_url_table()` + lazy HTTP store bypass the entire TVF SSRF / local-file guard

- **Dimension:** security
- **Status:** NEW surface (parallel path; the !190 `read_*` TVF guard is intact)
- **Location:** `crates/sqe-coordinator/src/session_context.rs:232`, `crates/sqe-catalog/src/lazy_object_store.rs:88-106`
- **Evidence:**
  ```rust
  // session_context.rs:232 — per-user authenticated session
  let ctx = ctx.enable_url_table();
  ```
  ```rust
  // lazy_object_store.rs:96-98 — any http/https registry miss builds a store, no allowlist
  Err(_) if matches!(url.scheme(), "http" | "https") => {
      build_and_register_http_store(&self.inner, url)
  }
  ```
  `TvfPolicy::check` (`sqe-core/src/config.rs:1224`) is invoked **only** from the `read_*` UDTFs
  (`read_parquet.rs:237`, `read_csv.rs:228`, `read_json.rs:93`, `read_delta.rs:93`).
  `SELECT * FROM '<url-or-path>'` resolves through DataFusion's `DynamicFileCatalog` ->
  `ListingTableFactory` -> `ObjectStoreRegistry`, never touching `TvfPolicy`.
- **Impact:** On the privileged coordinator pod, an authenticated user runs
  `SELECT * FROM 'https://internal-svc/data.json'` (SSRF from the coordinator's network position) or
  `SELECT * FROM '/mnt/secrets/cred.json'` / `'file:///var/run/secrets/.../token.json'` (local-file read
  bypassing the `allow_local_paths = false` default). Body is returned in the result set. Worst case is IMDS
  credential theft via `http://169.254.169.254/...`, caveated: DataFusion infers format from file extension,
  so extension-less IMDS paths may not resolve, and error sanitization blocks reading bodies via error text.
  The extension-bearing internal-HTTP SSRF and local-secret read are unconditional.
- **Fix:** Run quoted-string table names through `config.storage.tvf.check(...)` before a store is built
  (wrap the `DynamicFileCatalog` lookup), or disable `enable_url_table()` on the coordinator and require the
  guarded `read_*` TVFs. Minimum: make `LazyHttpObjectStoreRegistry` carry the `TvfPolicy` and reject
  non-allowlisted hosts in `build_and_register_http_store`.
- **Effort:** medium

---

### CAT-02 — high — Non-REST backends (Glue/HMS/JDBC/S3Tables) authenticate with the coordinator's shared service identity, not the user

- **Dimension:** security
- **Status:** NEW surface
- **Location:** `crates/sqe-catalog/src/rest_catalog.rs:505-599`, `:466`, `:485`
- **Evidence:**
  ```rust
  CatalogBackend::Glue { region, warehouse, endpoint } => {
      p.insert("aws.region".to_string(), region.clone());
      ("glue", "sqe-glue-session", p)   // line 546 — no bearer_token passed
  }
  ```
  ```rust
  let inner: Arc<dyn iceberg::Catalog> = Self::build_backend_catalog(backend).await?; // 466
  bearer_token: bearer_token.to_string(),   // 485 — stored for cache-key/logging only
  ```
- **Impact:** SQE's model is "no service account; every query runs as the authenticated user." For
  Glue/HMS/JDBC/S3Tables that holds only for the in-engine policy layer; the catalog + cloud-storage access
  runs under the pod's single IAM role / `AWS_PROFILE` / HMS connection, shared by all users. A shared
  cloud/storage identity gap, **not** a query-authz bypass (row/column policy, when wired, still runs above
  the catalog). Blast radius limited to deployments selecting a non-REST backend (REST/Polaris default passes
  the user bearer at `rest_catalog.rs:637`).
- **Fix:** Either add a loud startup WARN ("backend `glue` uses shared coordinator AWS credentials; per-user
  identity not enforced for storage") mirroring the existing ClientCredentials/Anonymous warnings, or thread
  per-user role assumption into `build_backend_catalog`. The comment at `rest_catalog.rs:418-419` already
  acknowledges the deferral. surface it to operators.
- **Effort:** medium

---

### CAT-03 — low — Dead `credential_vending` module ships a cross-tenant-unsafe cache keyed by table only

- **Dimension:** sustainability
- **Status:** NEW surface
- **Location:** `crates/sqe-catalog/src/credential_vending.rs:57-97`, `crates/sqe-catalog/src/lib.rs:15`
- **Evidence:**
  ```rust
  pub async fn get_or_extract(&self, table_key: &str, table_config: ...) -> VendedCredentials {
      if let Some(creds) = self.cache.get(table_key).await { ... return creds; } // keyed by table only
  ```
  `CredentialCache`/`get_or_extract`/`extract_from_table_config` have zero non-test callers. The live path is
  `rest_catalog.rs`'s `TableMetadataCache`, correctly keyed by `"{token_fingerprint}|{namespace}.{table_name}"`
  (`rest_catalog.rs:350`).
- **Impact:** No live exploit (unused). But it is `pub` and, if wired in, would serve User A's vended
  short-lived S3 creds to User B for the same table. Misleading doc-comment claims per-entry TTLs that `insert`
  never sets.
- **Fix:** Delete the module, or key by `(token_fingerprint, table_key)` and mark `#[doc(hidden)]` with a
  "not wired; do not use without per-user keying" note. Remove the stale TTL comment.
- **Effort:** trivial

---

### CAT-04 — low — Extra HEAD request to Polaris on every cold table load (catalog chattiness)

- **Dimension:** cost
- **Status:** NEW surface
- **Location:** `crates/sqe-catalog/src/rest_catalog.rs:937-948`, `:993-1015`
- **Evidence:**
  ```rust
  tokio::spawn(async move {
      let etag = fetch_table_etag_inner(&http_client, &bearer_token, &url).await; // 943
  });
  ```
  `fetch_table_etag_inner` issues `http_client.head(url)` (HEAD, not GET. does not re-download metadata).
- **Impact:** Doubles the request *count* (not bytes) to Polaris on every cold load. On a cold coordinator or
  after broad cache invalidation, a burst of extra round-trips. The HEAD is fire-and-forget and not gated by the
  circuit breaker, unlike the main load at `:924`.
- **Fix:** Capture the ETag from the main load's response headers instead of re-requesting, or fold it into the
  existing ETag revalidation GET (`:867`); skip when the soft TTL makes revalidation rare.
- **Effort:** small

---

### CAT-05 — info — `credential_vending` static-cred fallback leaks secret into a plain `String` (dead code)

- **Dimension:** security
- **Status:** NEW surface
- **Location:** `crates/sqe-catalog/src/credential_vending.rs:91-96`
- **Evidence:**
  ```rust
  secret_key: self.storage_config.s3_secret_key.expose().to_string(), // 93
  ```
- **Impact:** `VendedCredentials.secret_key` is a plain `String` (no `SecretString`/`Zeroize`, and
  `#[derive(Debug)]` at line 16 would print it). Dead code, hence info. but if wired in, the secret leaves the
  `SecretString` boundary.
- **Fix:** If retained, type as `SecretString` with a redacting `Debug` impl; otherwise resolved by deleting
  the module (CAT-03).
- **Effort:** trivial
