# Findings — Dependencies, Build, CI, Deploy, Cost & Sustainability (cross-cutting)

**Scope:** Workspace `Cargo.toml`/`Cargo.lock` (parsed directly, since `cargo deny` cannot reach a registry
offline), `deny.toml`, the vendored `iceberg-rust` fork, the three Dockerfiles + `.dockerignore`,
`.gitlab-ci.yml` and `.github/`, the `deploy/helm/sqe/` chart, and tree-wide sustainability signals. Confirmed
several prior fixes still hold: all 8 `danger_accept_invalid_certs` sites read a config field defaulting to
`false`; the catalog feature-gating in `sqe-catalog/Cargo.toml` is genuinely tight (REST-only default,
AWS/Thrift/sqlx behind opt-in features); the Helm pods set a solid `securityContext` (runAsNonRoot, drop ALL
caps); the release profile (`lto="thin"`, `strip=true`) is well tuned; and the example-secret files contain only
placeholders. `cargo audit` is clean (only the allowed `paste` warning).

> Note: DEP-03/DEP-04 are auth-crate reliability issues surfaced by this agent's dependency/timeout sweep; they
> complement the `findings-auth-policy.md` set rather than duplicating it.

---

### DEP-01 — high — Helm pods set `readOnlyRootFilesystem: true` but the engine spills (and persists sessions) to `/tmp`, which is unwritable

- **Dimension:** reliability
- **Status:** NEW surface
- **Location:** `deploy/helm/sqe/templates/coordinator-deployment.yaml:39-43`, `deploy/helm/sqe/templates/worker-deployment.yaml:40-44`, `crates/sqe-core/src/config.rs:591` (`spill_to_disk: true`), `:1917`, `:1931`, `:1892`
- **Evidence:**
  ```yaml
  securityContext:
    allowPrivilegeEscalation: false
    readOnlyRootFilesystem: true
    capabilities: { drop: ["ALL"] }
  # ...volumes: only the read-only ConfigMap is mounted; no emptyDir for /tmp
  ```
  ```rust
  spill_to_disk: true,                                   // default ON
  fn default_spill_dir() -> String { "/tmp/sqe-spill".to_string() }
  fn default_coordinator_spill_dir() -> String { "/tmp/sqe-coordinator-spill".to_string() }
  ```
- **Impact:** `readOnlyRootFilesystem: true` makes the entire container FS (including `/tmp`) unwritable, yet
  `spill_to_disk` defaults to `true` and the spill/session paths default to `/tmp`. The first large join/sort that
  exceeds `memory_limit` tries to spill, the write to `/tmp/sqe-spill` fails with EROFS, and the query errors out
  (or the pod panics). Spill-to-disk is the engine's defense against OOM, silently disabled in the recommended
  deployment exactly when it is needed. Session persistence to `/tmp/sqe-sessions.json` also fails.
- **Fix:** Add an `emptyDir` volume mounted at the spill dir and `/tmp` in both deployment templates; keep
  `readOnlyRootFilesystem: true`. Alternatively render `spill_to_disk = false` when no writable volume is provided.
- **Effort:** small

---

### DEP-02 — high — Helm coordinator pod memory limit (2Gi) is below the engine's default tracked `memory_limit` (8GB) -> OOMKill before spill triggers

- **Dimension:** reliability
- **Status:** NEW surface
- **Location:** `deploy/helm/sqe/values.yaml:15` (`limits.memory: "2Gi"`), `deploy/helm/sqe/templates/configmap.yaml:9-15` (no coordinator `memory_limit` rendered), `crates/sqe-core/src/config.rs:1916` (`default_coordinator_memory() -> "8GB"`)
- **Evidence:**
  ```yaml
  # values.yaml coordinator
  limits: { memory: "2Gi" }
  ```
  ```rust
  fn default_coordinator_memory() -> String { "8GB".to_string() }
  ```
- **Impact:** The configmap never renders a coordinator `memory_limit`, so the coordinator falls back to the code
  default of **8GB** of tracked query memory inside a pod capped at **2Gi**. DataFusion allocates up to its 8GB
  accounting budget; the kernel OOMKills the pod at 2Gi long before the engine's spill threshold fires. Worker pods
  have the inverse: pod limit `8Gi` (`values.yaml:32`) == engine `memory_limit "8GB"` (`values.yaml:55`), leaving
  zero headroom for Arrow buffers outside DataFusion's accounted pool, so workers also OOMKill under load.
- **Fix:** Render a coordinator `memory_limit` in `configmap.yaml` derived from the pod limit (~70-75% of
  `coordinator.resources.limits.memory`), and set the worker engine `memory_limit` to ~75% of its pod limit. Add
  the coordinator `memory_limit`/`spill_dir` keys to `values.yaml`.
- **Effort:** small

---

### DEP-03 — medium — Auth-path HTTP clients to the IdP have no request timeout; a slow/hung IdP blocks logins indefinitely

- **Dimension:** reliability
- **Status:** NEW surface
- **Location:** `crates/sqe-auth/src/oidc_provider.rs:68-70`, `oidc_password.rs:38`, `oauth.rs:37`, `oidc_discovery.rs:38`, `token_exchange.rs:119`
- **Evidence:**
  ```rust
  // oidc_provider.rs — no .timeout(...) on the builder, no per-request timeout on .send()
  let client = reqwest::Client::builder()
      .danger_accept_invalid_certs(config.accept_invalid_certs)
      .build()
  ```
- **Impact:** Five auth-provider reqwest clients are built with no connect or request timeout. Every Flight
  SQL/Trino login hits the IdP token endpoint through one of these. A slow or hung IdP (or TCP black-hole) leaves
  each auth request hanging for the OS default (minutes), pinning a Tokio task and a connection per attempt. Under
  a login burst against a degraded IdP this exhausts the coordinator's task/connection budget and stalls the whole
  auth surface. a self-inflicted DoS. (`oidc_m2m.rs`, `bearer_token.rs`, `opa.rs`, `polaris.rs` already set
  `.timeout(...)`.)
- **Fix:** Add `.timeout(Duration::from_secs(10))` and `.connect_timeout(Duration::from_secs(5))` to each of the
  five `Client::builder()` chains.
- **Effort:** trivial

---

### DEP-04 — medium — `&body[..500]` byte-slice on IdP error bodies panics on a UTF-8 char boundary

- **Dimension:** reliability
- **Status:** NEW surface
- **Location:** `crates/sqe-auth/src/oidc_provider.rs:114`, `:163`, `oidc_password.rs:81`, `:125`, `token_exchange.rs:193`
- **Evidence:**
  ```rust
  let body = if body.len() > 500 {
      format!("{}...[truncated]", &body[..500])   // byte index, not char boundary
  } else { body };
  ```
- **Impact:** When the IdP rejects credentials, the error body is truncated by slicing at byte index 500. If byte
  500 lands mid-codepoint. routine for any localized error page or JSON in UTF-8 over 500 bytes. the slice panics
  with "byte index is not a char boundary". This is the auth failure path, so an ordinary localized IdP error
  response (not an adversary) crashes the request task. Five call sites.
- **Fix:** Replace `&body[..500]` with `body.chars().take(500).collect::<String>()` or
  `body.get(..500).unwrap_or(&body)`. Factor into a helper.
- **Effort:** trivial

---

### DEP-05 — medium — `deny.toml` documents a stale vendored-fork rev; the "verify the SHA" instruction would fail

- **Dimension:** sustainability
- **Status:** NEW surface
- **Location:** `deny.toml:5-8`, vs `Cargo.toml:56` and `vendor/iceberg-rust/README.md`
- **Evidence:**
  ```toml
  # deny.toml
  #   github.com/risingwavelabs/iceberg-rust.git rev=1978911ec4
  # The pinned rev mitigates force-push attacks. Verify the SHA matches:
  #   git ls-remote ... | grep 1978911
  ```
  ```toml
  # Cargo.toml:56 (and vendor/iceberg-rust/README.md: "@ c034b19105fa (2026-06-02)")
  # Pinned to risingwavelabs/iceberg-rust dev_rebase_main_20260303 @ c034b19105fa.
  ```
- **Impact:** The supply-chain anchor for the vendored Iceberg fork is documented in two places that disagree.
  `deny.toml` pins `1978911ec4`; the actual vendored tree is `c034b19105fa`. Anyone following deny.toml's own
  verification step (`grep 1978911`) validates the wrong commit, defeating the stated force-push mitigation. The
  vendored copy has no `.git`, so the rev can only be trusted via this (stale) documentation.
- **Fix:** Update `deny.toml`'s rev comment and the `grep` example to `c034b19105fa`, and add a CI/release check
  asserting the deny.toml rev string equals the one in `vendor/iceberg-rust/README.md`.
- **Effort:** trivial

---

### DEP-06 — medium — Vulnerable legacy TLS/HTTP stack (rustls 0.21 / rustls-webpki 0.101.7 / hyper 0.14) ships duplicated via the AWS SDK connector

- **Dimension:** sustainability
- **Status:** NEW surface
- **Location:** `Cargo.lock` (`aws-smithy-http-client 1.1.12` pulls both stacks), gated by `sqe-catalog` features `glue`/`s3tables` (`crates/sqe-catalog/Cargo.toml:64-79`); 3 advisories ignored in `deny.toml:26-34`
- **Evidence:**
  ```
  # aws-smithy-http-client 1.1.12 dependency list (Cargo.lock):
  h2 0.3.27 + h2 0.4.13, http 0.2.12 + http 1.4.0, hyper 0.14.32 + hyper 1.9.0,
  hyper-rustls 0.24.2 + 0.27.9, rustls 0.21.12 + rustls 0.23.40, tokio-rustls 0.24.1 + 0.26.4
  # rustls 0.21.12 -> rustls-webpki 0.101.7  (RUSTSEC-2026-0098/0099/0104, ignored)
  ```
- **Impact:** The AWS Glue / S3 Tables catalog backends drag the AWS SDK's legacy connector, which links a complete
  second TLS+HTTP stack: `rustls 0.21`, `rustls-webpki 0.101.7` (three cert-validation advisories), `hyper 0.14`,
  `h2 0.3`, `http 0.2`. The Polaris REST path correctly uses the patched `rustls 0.23` / webpki 0.103. So any
  `Dockerfile.full` / `--features glue,s3tables` image carries a known-vulnerable cert-validation path plus ~double
  the TLS/HTTP compile and binary weight. This matches deny.toml's own scoped-exposure note. tracked-but-unaddressed,
  not novel, but a live cert-validation exposure in the full image and the single largest source of
  duplicate-major-version bloat (~55 crates ship two-or-more versions out of ~772 total).
- **Fix:** Build an explicit modern `HttpClient` (hyper 1 / rustls 0.23) for the vendored iceberg Glue/S3Tables
  backends so `aws-config` stops selecting the legacy default connector, dropping rustls 0.21 / webpki 0.101.7 /
  hyper 0.14; then remove the three RUSTSEC ignores. Until then, document that `Dockerfile.full` /
  `--features glue,s3tables` images carry the advisory.
- **Effort:** large

---

### DEP-07 — low — `delta` feature pulls a second `reqwest` (0.13) and a second arrow/parquet tree via `deltalake-core`

- **Dimension:** sustainability
- **Status:** NEW surface
- **Location:** `crates/sqe-catalog/Cargo.toml:137` (`deltalake-core = "0.32.1"`), enabled by default in `crates/sqe-cli/Cargo.toml:23` (`features = ["rest", "sql-sqlite", "hadoop", "delta"]`); `Cargo.lock` `buoyant_kernel 0.21.103`
- **Evidence:**
  ```toml
  deltalake-core = { version = "0.32.1", features = ["datafusion", "rustls", "cloud"], optional = true }
  # Cargo.lock: deltalake-core -> buoyant_kernel 0.21.103 -> reqwest 0.13.3 (2nd reqwest major), arrow, parquet, rand 0.9.4
  ```
- **Impact:** Enabling `delta` (which the embedded CLI does by default) brings `deltalake-core` and its dependency
  `buoyant_kernel`, which pulls a **second** `reqwest` major (0.13 alongside the workspace's 0.12) plus its own
  arrow/parquet/rand surfaces. Extra compile time, binary size, and a wider transitive attack surface for a
  read-only Delta add-on. The unusually named `buoyant_kernel` crate's provenance could not be assessed offline.
- **Fix:** Keep `delta` off the cluster default (it already is). Consider dropping it from the embedded CLI default
  unless Delta read is core. When online, confirm `buoyant_kernel`'s publisher/maintenance and whether a newer
  `deltalake-core` collapses onto reqwest 0.12.
- **Effort:** small

---

### DEP-08 — low — Helm chart defaults to `image.tag: latest` with `pullPolicy: IfNotPresent` — non-reproducible deploys

- **Dimension:** sustainability
- **Status:** NEW surface
- **Location:** `deploy/helm/sqe/values.yaml:1-4`
- **Evidence:**
  ```yaml
  image: { repository: sqe, tag: latest, pullPolicy: IfNotPresent }
  ```
- **Impact:** `tag: latest` means every install resolves to whatever `latest` points at that moment. two clusters
  installed a day apart can run different binaries with no record. `IfNotPresent` then means a node that already
  cached *a* `latest` never re-pulls, so an "upgrade" silently keeps the stale image. No pinned digest to roll back
  to. For a security-sensitive query engine this defeats reproducible deploys and incident forensics.
- **Fix:** Default `tag` to the chart `appVersion` (a concrete version), document pinning by `@sha256:` digest, and
  use `pullPolicy: IfNotPresent` only for digest-pinned tags.
- **Effort:** trivial

---

### DEP-09 — low — CI compiles `cargo-audit` from source on every check run (uncached) and `|| true` can silently skip the security scan

- **Dimension:** cost
- **Status:** NEW surface
- **Location:** `.gitlab-ci.yml:38-39`, cache block `:57-62`
- **Evidence:**
  ```yaml
  - cargo install cargo-audit --quiet 2>/dev/null || true
  - cargo install cargo-deny --quiet 2>/dev/null || true
  # cache.paths: target/, .cargo/registry/, .cargo/bin/cargo-deny  (cargo-audit binary NOT cached)
  ```
- **Impact:** The `cargo-check` job `cargo install`s both tools every run. The cache persists only
  `.cargo/bin/cargo-deny`, so `cargo-audit` recompiles from source each time (multi-minute build), and the
  `|| true` swallows any failure. a broken install silently skips the `cargo audit` security gate without failing
  the pipeline. Wasted CI compute on every relevant push, plus a security gate that can silently no-op.
- **Fix:** Cache `.cargo/bin/` wholesale (or both binaries), use `cargo binstall` for prebuilt binaries, or bake
  both into a CI base image. Remove the `|| true` on the security tools so a failed install fails the job.
- **Effort:** small

---

### DEP-10 — info — `deploy/sqe.env.example` ships a placeholder client secret that reads like a real value

- **Dimension:** security
- **Status:** NEW surface
- **Location:** `deploy/sqe.env.example:5`
- **Evidence:**
  ```
  SQE_CLIENT_SECRET=sqe-secret-change-me
  ```
- **Impact:** A clear placeholder, but a syntactically valid secret that gets copied into a real `.env` and used to
  register a Keycloak client. The common failure mode is shipping the placeholder unchanged to a non-dev
  environment, leaving the SQE OIDC client protected by a publicly known secret. (`.env.example` and
  `sqe.toml.example` contain only `<your-...>` placeholders and the public quickstart `iceberg:iceberg` -- fine.)
- **Fix:** Change the value to an obviously-invalid sentinel like `CHANGEME_DO_NOT_USE_IN_PROD`, and add a startup
  check that refuses to boot with the placeholder secret outside dev mode.
- **Effort:** trivial
