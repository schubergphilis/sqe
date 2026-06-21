# SQE Audit 2026-06-04 — Shared Brief for Audit Agents

You are one of several parallel agents auditing the SQE (Sovereign Query Engine)
codebase. SQE is a distributed SQL engine for Apache Iceberg: Coordinator (SQL
parse, auth, policy, scheduling) -> Workers (DataFusion) -> Iceberg (Polaris REST
+ S3). Auth model: no service account; every query runs as the authenticated user
via OIDC -> bearer token passthrough. Security via LogicalPlan rewriting before
DataFusion optimization (row filters, column masks).

## Your job

Find REAL, NEW issues across the dimensions assigned to you, in your assigned
scope only. Quality over quantity, but do not artificially cap the count. Every
issue MUST quote the exact `file:line` evidence that proves the problem. No quote,
no issue. Security false-positives waste triage time — be precise.

## The five dimensions (tag every finding with exactly one)

- **security** — auth bypass, SSRF, path traversal, injection, secret leakage,
  XSS, missing authz, info disclosure, untrusted-input parsing, crypto misuse.
- **performance** — hot-path allocations, O(n^2), blocking calls in async,
  redundant work, missing pushdown, lock contention, unbounded buffering.
- **reliability** — panics reachable from user/network input (`unwrap`/`expect`/
  `panic!`/`unreachable!`/indexing/slicing/integer overflow on untrusted data),
  SPOFs, missing timeouts, unbounded growth, silent failures / swallowed errors,
  poisoned locks, race conditions, missing backpressure.
- **cost** — cloud/compute spend: S3 request amplification, network egress,
  redundant scans, over-fetching, no caching, oversized instances/containers,
  chatty catalog calls.
- **sustainability** — resource efficiency (wasted CPU/RAM/allocations) AND
  long-term maintainability: dead code, large duplicated blocks, tech debt,
  TODO/FIXME density, dependency health/bloat, bus-factor, test gaps on new code.

## Severity (use these exact words)

`critical` (exploitable now, high blast radius) / `high` / `medium` / `low` /
`info`.

## Issue schema — write each finding EXACTLY like this

```
### <ID> — <severity> — <one-line title>

- **Dimension:** security | performance | reliability | cost | sustainability
- **Status:** NEW surface  |  REGRESSION of resolved finding
- **Location:** `path/to/file.rs:LINE` (add more lines if relevant)
- **Evidence:**
  ```rust
  // the exact offending line(s), quoted
  ```
- **Impact:** what an attacker/operator/bill actually suffers; blast radius.
- **Fix:** concrete, specific remediation (not "add validation").
- **Effort:** trivial | small | medium | large
```

Use your assigned ID prefix and number sequentially (e.g. `AUTH-01`, `AUTH-02`).

## Prior audits — DO NOT re-report these (they are RESOLVED)

Four prior audit passes already fixed ~190 findings. Do not restate any of these
unless you can prove a REGRESSION by quoting the current offending line:

- TLS on Flight SQL (`[coordinator.tls]`, optional mTLS) — added & enforced.
- Error sanitization: `SqeError::client_message()` / `sqe_error_to_status()` —
  no raw DataFusion/iceberg errors to clients; details logged server-side.
- JWT `exp` + audience validation; JWKS caching; WARN when audience unconfigured.
- Session context cache keyed by token fingerprint (SHA-256), not username (S1).
- AnonymousProvider: startup WARN when active (S2).
- ClientCredentials shared-token: documented + startup WARN (S3).
- OIDC error bodies NOT returned verbatim to clients (S4).
- `ssl_verification = false` -> startup WARN; code default is `true`.
- Query size limit enforced before parsing (DoS).
- Audit log: query SHA-256 hash alongside text; tokens never logged.
- Rate limiting (`governor`) on Trino HTTP and Flight paths.
- K8s securityContext in Helm templates.
- `rsa` crate removed (Marvin); `paste` unmaintained advisory ignored in deny.toml.
- TVF SSRF / path traversal (`read_parquet('http://169.254.169.254')`,
  `read_parquet('/etc/shadow')`) closed in MR !190 with a guard. CLI `next_uri`
  same-origin validation exists.
- Constant-time comparison for auth secrets; credential redaction in Debug impls.
- A 130-issue wave (2026-05-15) fixed ~110 issues across 19 MRs (!195 -> !213).

**Weight your effort on NEW surface since 2026-05-15:** web UI (`web_ui.rs`,
`web_ui/`), quack protocol (`sqe-quack-*`), `sqe-trino-functions`, `sqe-lineage`,
embedded CLI (`sqe-cli/embedded.rs`), file/S3 table functions
(`read_csv/json/parquet/delta`, `file_tvf_common`, `lazy_object_store`, `s3_io`),
and cloud catalogs (glue/hms/sql/s3tables/loader).

## Specific leads to verify (frame as VERIFY, not assume)

- TVF guard regression: commits `0ee68c0` ("embedded engine permits local-file
  TVF reads") and `03b010c` may have re-opened the !190 SSRF/path-traversal hole.
  Check: does the guard still cover the **coordinator / Flight SQL server** path,
  or do only the embedded-CLI bypasses skip it? Read the actual guard code.
- Web UI defaults ON (`[metrics] web_ui` default true). Are ALL web routes
  gated/authenticated, or is the dashboard exposed unauthenticated? Is query
  SQL/PII shown? Is HTML output escaped against XSS?
- 8 `danger_accept_invalid_certs` call sites in `sqe-auth` — are they all gated
  behind config that defaults to secure + warns?

## Environment notes

- Offline: no network. `cargo audit` is clean (only the allowed `paste` warning).
  `cargo deny` bans/licenses cannot fetch the registry — parse `Cargo.lock`
  directly for duplicate major versions if you need dependency data.
- `cargo build`/`clippy` may be slow/unavailable offline; prefer reading source.

## Output

Create your file at `docs/audit/2026-06-04/findings-<your-area>.md` with a short
1-paragraph scope note at top, then your issues in the schema above. Then return
to the dispatcher a compact list: `<ID> | severity | dimension | title`.
