# Findings — Auth & Policy (`sqe-auth`, `sqe-policy`)

**Scope:** All of `crates/sqe-auth/src/` and `crates/sqe-policy/src/`, security-weighted,
plus the coordinator wiring that decides whether the policy crate runs
(`sqe-coordinator/src/{main.rs,bin/sqe_server.rs}`) and auth/policy config defaults in
`sqe-core/src/config.rs`. Verified: all 8 `danger_accept_invalid_certs` sites (gated,
default secure, coordinator warns at startup, with one uncovered flag in AUTH-04), JWT/JWKS
validation (RS256 pinned, `kid` required, `exp`/`aud`/`iss` enforced. no alg-confusion or
alg=none hole), the loud startup warnings for Anonymous/BearerPassthrough/ClientCredentials,
the AuthChain fail-closed control flow, and constant-time API-key comparison. No plaintext
token logging found in sqe-auth.

> **AUTH-01 independently verified by the dispatcher** against `main.rs:175-176`,
> `sqe_server.rs:560-561`, and `README.md:39,51`.

---

### AUTH-01 — critical — Policy enforcement is hardcoded to passthrough; `policy.engine = "opa"` is silently ignored (total fail-open)

- **Dimension:** security
- **Status:** NEW surface
- **Location:** `crates/sqe-coordinator/src/main.rs:175-176`, `crates/sqe-coordinator/src/bin/sqe_server.rs:560-561`, `crates/sqe-core/src/config.rs:2335` (`engine` only ever read for env override), `README.md:39,51`
- **Evidence:**
  ```rust
  // main.rs:174-176 — identical at sqe_server.rs:559-561
  // Initialize policy (passthrough)
  let policy_enforcer: Arc<dyn sqe_policy::PolicyEnforcer> =
      Arc::new(sqe_policy::PassthroughEnforcer);
  ```
  ```rust
  // main.rs:244 — confirms it is never wired
  None, // policy_store — wired when policy engine is enabled
  ```
  `config.policy.engine` is parsed/validated (`config.rs:1507` rejects unknown values) and
  env-overridable (`config.rs:2335`) but is **never read to construct an enforcer**.
  README advertises it as shipped: `README.md:39` "Row filters and column masks via OPA or
  Cedar are enforced at the LogicalPlan layer", `README.md:51` matrix "OPA / Cedar policy at
  LogicalPlan | yes".
- **Impact:** Every row filter, column mask, and restriction in `sqe-policy` is unreachable
  on the live read path. An operator sets `policy.engine = "opa"`, config-load succeeds, no
  startup warning fires (the same startup path loudly warns about TLS, rate-limiting, and
  unauthenticated workers, but is silent here), and every query runs with zero policy
  enforcement. Full read of restricted rows and unmasked PII for every authenticated user.
  Blast radius: every deployment relying on SQE-side policy rather than catalog ACLs. Misleading
  documentation that pushes operators into an insecure config is explicitly in scope per `SECURITY.md`.
- **Fix:** Build the enforcer from `config.policy.engine` in both entry points (`Opa` ->
  `PolicyPlanRewriter::new(Arc::new(OpaStore::with_config(...))).with_mask_key(...)`). In
  `validate()`, error when `engine == Opa` but `[policy.opa]`/`opa_url` is unset, and reject
  `Cedar` until implemented. Until wired, change README "enforced" -> "roadmap" and emit a
  startup `error!` whenever `policy.engine != Passthrough`.
- **Effort:** medium

---

### AUTH-02 — medium — JWKS URL not constrained to HTTPS (key substitution / identity forgery over plaintext)

- **Dimension:** security
- **Status:** NEW surface
- **Location:** `crates/sqe-auth/src/bearer_token.rs:205-212` (fetch); no scheme check at `bearer_token.rs:114-159`
- **Evidence:**
  ```rust
  // bearer_token.rs:205-212 — jwks_url used verbatim, any scheme accepted
  let response = self.client.get(&self.config.jwks_url).send().await
      .map_err(|e| { AuthError::Internal(anyhow::anyhow!("JWKS fetch failed: {e}")) })?;
  ```
- **Impact:** If `jwks_url` is `http://...` (or `https://` with `accept_invalid_certs = true`),
  an on-path attacker substitutes the JWKS with their own RSA public key, then mints a JWT with
  any `sub`/`roles` that SQE accepts as valid. The bearer is also passed through to Polaris/S3 as
  the catalog token, so the forged identity drives both SQE roles and the downstream principal.
  Highest-trust input in the auth path, fetched over an unauthenticated channel.
- **Fix:** Reject a non-`https` `jwks_url` in `BearerTokenProvider::new` unless an explicit
  `allow_insecure_jwks` opt-in is set (mirroring the existing `allow_unbounded_audience` pattern),
  with a startup warning when used.
- **Effort:** small

---

### AUTH-03 — low — `InMemoryPolicyStore` resolves the FIRST matching role only (least-privilege violation)

- **Dimension:** security
- **Status:** NEW surface
- **Location:** `crates/sqe-policy/src/policy_store.rs:74-84`
- **Evidence:**
  ```rust
  for role in &user.roles {
      if let Some(policy) = role_policies.get(role) {
          return Ok(policy.clone());   // first matching role wins, others ignored
      }
  }
  ```
- **Impact:** A multi-role user (e.g. `["analyst","restricted"]`) gets only the first iterated
  role's policy; a stricter policy on a later role is silently dropped. `user.roles` ordering is
  not normalized, so the effective policy is order-dependent and can over-grant. Documented as
  test/dev but selectable as `PolicyEngine::InMemory`, so reachable once AUTH-01 is fixed by wiring it.
- **Fix:** Union all matching role policies (union of restricted_columns and column_masks, AND of
  row_filters), or refuse to select `InMemory` outside a dev flag.
- **Effort:** small

---

### AUTH-04 — low — `auth.external.accept_invalid_certs` skips TLS verification with no startup warning

- **Dimension:** security
- **Status:** NEW surface
- **Location:** `crates/sqe-core/src/config.rs:908-909`; consumed at `crates/sqe-coordinator/src/main.rs:394` and `sqe_server.rs:1212`; startup warning at `main.rs:125-127` / `sqe_server.rs:456-458` checks only `should_skip_tls_verify()`
- **Evidence:**
  ```rust
  // config.rs:908-909 — separate flag, NOT part of should_skip_tls_verify()
  #[serde(default)]
  pub accept_invalid_certs: bool,
  ```
  `should_skip_tls_verify() = tls_skip_verify || !ssl_verification` (config.rs:853-855) does not
  include `external.accept_invalid_certs`, so the startup warning never fires for it.
- **Impact:** Setting `[auth.external] accept_invalid_certs = true` disables cert verification for
  OIDC discovery and the interactive auth-code/device-code token endpoints, with zero startup signal
  (unlike the main auth path). MITM of IdP discovery/token responses for browser/device login.
  Default is secure (`false`), so this is a missing-warning gap, not a broken default.
- **Fix:** Add `|| config.auth.external.as_ref().map_or(false, |e| e.accept_invalid_certs)` to the
  TLS-verification startup warning in both binaries.
- **Effort:** trivial

---

### AUTH-05 — low — JWKS signature-retry decision uses error-string substring match (brittle on key rotation)

- **Dimension:** reliability
- **Status:** NEW surface
- **Location:** `crates/sqe-auth/src/bearer_token.rs:310-317`
- **Evidence:**
  ```rust
  let err_str = e.to_string();
  if !err_str.contains("InvalidSignature") {
      return Err(Self::map_jwt_error(e));
  }
  ```
- **Impact:** Whether to refetch the JWKS on a possible key rotation is decided by substring-matching
  the `Display` text of `jsonwebtoken::errors::Error`, not its `ErrorKind`. If upstream wording changes,
  legitimate signature failures during a real key rotation stop triggering the refetch and valid clients
  are rejected until the 15-min TTL expires. The correct `e.kind()` API is already used in `map_jwt_error`
  (bearer_token.rs:337-353).
- **Fix:** Match on `e.kind() == ErrorKind::InvalidSignature`.
- **Effort:** trivial

---

### AUTH-06 — info — Polaris management-API error bodies carried verbatim in `SHOW GRANTS` errors

- **Dimension:** security
- **Status:** NEW surface
- **Location:** `crates/sqe-policy/src/grants/polaris.rs:518-523` (and sibling `format!("... {text}")` sites)
- **Evidence:**
  ```rust
  let text = resp.text().await.unwrap_or_default();
  return Err(sqe_core::SqeError::Execution(format!(
      "Failed to list catalog roles (HTTP {status}): {text}"
  )));
  ```
- **Impact:** Raw Polaris management-API bodies (which may name internal roles/catalogs) are embedded in
  the error string. Whether it reaches the client depends on `SqeError::client_message()` sanitization
  (resolved S4), so likely scrubbed at the boundary. flagged info because the verbatim body is carried at
  all. The calls are correctly authorized (each uses the caller's own `bearer_auth(&token)`), and
  `validate_url_identifier` (polaris.rs:31-45) blocks path-traversal in interpolated names.
- **Fix:** Log the body server-side; return a generic "Polaris returned HTTP {status}" to the client.
- **Effort:** trivial

---

### AUTH-07 — medium — Unknown-`kid` bearer token forces unconditional JWKS cache invalidation + refetch (pre-auth DoS / cache defeat)

- **Dimension:** reliability
- **Status:** NEW surface
- **Location:** `crates/sqe-auth/src/bearer_token.rs:304-334` (fall-through to `refetch_jwks`), `bearer_token.rs:259-263` (`refetch_jwks` invalidates then fetches)
- **Evidence:**
  ```rust
  if let Some(decoding_key) = jwks.get(&kid) { /* try decode */ }
  else { debug!(kid = %kid, "Key ID not found in cached JWKS, refetching"); }
  // falls through unconditionally:
  let jwks = self.refetch_jwks().await?;
  ```
  ```rust
  async fn refetch_jwks(&self) -> Result<Arc<JwksMap>, AuthError> {
      self.jwks_cache.invalidate(Self::CACHE_KEY).await;  // wipes good cache FIRST
      self.fetch_and_cache_jwks().await
  }
  ```
  Reachability is pre-auth: `detect_jwt` (bearer_token.rs:394-412) only checks the `eyJ` prefix, and
  `decode_header` succeeds on an attacker-crafted unsigned header `{"alg":"RS256","kid":"random"}`.
- **Impact:** Each bearer with an unknown `kid` triggers one outbound JWKS GET (1:1 amplification, SQE
  hammering the IdP), and `refetch_jwks` invalidates the single cache entry **before** fetching, so a stream
  of bad-`kid` requests wipes the good cache for legitimate users; if a refetch then fails (IdP briefly down)
  the cache is left empty and all bearer auth fails. `fetch_mutex` serializes concurrency but not rate. The
  `AuthRateLimiter` bounds the amplification when rate-limiting is enabled (itself optional), but does not fix
  the global-cache-invalidation harming legitimate users.
- **Fix:** Match on `e.kind()` for the retry (shares root with AUTH-05); cap/rate-limit refetches independent
  of the request limiter; and merge newly fetched keys into the existing map instead of invalidate-then-fetch,
  so good keys are never dropped until a new fetch succeeds.
- **Effort:** small
