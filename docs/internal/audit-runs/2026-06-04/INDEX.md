# SQE Audit — 2026-06-04

**Scope:** Full workspace (17 crates, ~140K lines Rust) at version **0.31.4**.
**Dimensions:** security, performance, reliability, cost, sustainability.
**Method:** 8 parallel scoped review agents, each de-duplicated against the four prior
audit passes (`AUDIT.md`, `docs/issues.md`, `docs/security_audit.md`, and the 2026-05-15
130-issue wave). Mechanical scans first (`cargo audit` clean; reliability greps for
`unwrap`/`panic`/`unsafe`). Every finding quotes `file:line` evidence. The four critical
findings (AUTH-01, CAT-01, QUACK-01, QUACK-02), plus COORD-01 and PLAN-01, were
independently re-verified against source by the lead.

This audit deliberately weights the **959 commits of new surface** added since the last
big audit (2026-05-15): the web UI, the Quack wire protocol, `sqe-trino-functions`,
`sqe-lineage`, the embedded CLI, file/S3 table functions, and cloud catalogs. Findings
that merely restate a resolved item are excluded; one regression is flagged as such (SQL-06,
CORE-01).

> These are draft issues for triage, to be converted into tickets. Each `###` section in a
> findings file maps to one ticket. Severities are the auditor's; re-rank during triage.

---

## Totals

**61 findings** + 3 verified-safe notes (64 items total) across 8 areas.

| Severity | Findings |
|---|---|
| critical | 4 |
| high | 15 |
| medium | 17 |
| low | 19 |
| info | 6 |

| Dimension | Findings |
|---|---|
| security | 22 |
| reliability | 27 |
| performance | 4 |
| cost | 3 |
| sustainability | 5 |

| Area file | Items |
|---|---|
| [findings-auth-policy.md](findings-auth-policy.md) | AUTH-01..07 (7) |
| [findings-catalog.md](findings-catalog.md) | CAT-01..05 (5) |
| [findings-web-flight.md](findings-web-flight.md) | WEB-01..06 (6; WEB-06 is a verified-safe note) |
| [findings-coordinator-core.md](findings-coordinator-core.md) | COORD-01..06 (6) |
| [findings-quack-worker.md](findings-quack-worker.md) | QUACK-01..12 (12) |
| [findings-sql-trino-lineage.md](findings-sql-trino-lineage.md) | SQL-01..08 (8) |
| [findings-planner-core.md](findings-planner-core.md) | PLAN-01..05, CORE-01..03, MET-01, CLI-01 (10; MET-01 + CLI-01 are verified-safe notes) |
| [findings-deps-infra.md](findings-deps-infra.md) | DEP-01..10 (10) |

Verified-safe notes (counted separately, not findings): **WEB-06** (dashboard XSS escaping
correct), **MET-01** (metrics cardinality bounded), **CLI-01** (CLI clean for scope).

---

## Fix these first (criticals + highest-blast-radius highs)

1. **AUTH-01 (critical)** — OPA/Cedar policy enforcement is never constructed; the coordinator
   hardcodes `PassthroughEnforcer`. `policy.engine = "opa"` is silently ignored. Total fail-open
   for every row filter and column mask, while the README advertises it as enforced. *(Verified.)*
2. **CAT-01 (critical)** — `enable_url_table()` + the lazy HTTP/S3 object store let any
   authenticated user run `SELECT * FROM 'http://internal/...'` / `'/mnt/secrets/...'` on the
   coordinator, bypassing the `TvfPolicy` SSRF/path-traversal guard entirely. *(Verified.)*
3. **QUACK-01 / QUACK-02 (critical)** — the Quack wire decoder runs **pre-authentication**
   (`decode_message` at `app.rs:76`, before `authenticate` at `:267`) and trusts wire-supplied
   depth/count fields: nested types recurse to a stack-overflow process abort; unbounded counts
   pre-allocate to an OOM process abort. One small unauthenticated request kills the coordinator
   process. *(Verified: no depth/size guard in `data_chunk.rs`.)*
4. **PLAN-01 (high, consider critical)** — `StarSchemaReorderRule` is ON by default and rebuilds
   multi-join plans by re-resolving keys *by column name*; with the duplicate column names typical
   of star schemas it binds to the wrong table's column, returning **silently wrong rows**. Data
   correctness on a default-on path. *(Verified rule is wired + default-on.)*
5. **WEB-01 / WEB-02 / WEB-03 (high)** — the ops dashboard + all `/api/v1/*` are served
   unauthenticated on `0.0.0.0` by default, exposing every user's SQL/usernames; the prepared-
   statement put/close Flight handlers skip auth, enabling cross-session parameter poisoning.
6. **QUACK-05 / QUACK-07 (high)** — the Quack endpoint and the worker Flight service have **no TLS
   path at all**, sending bearer tokens and live S3 credentials in cleartext.
7. **COORD-01 (high)** — the result-cache invalidation key (bare `sales`) never matches the index
   key (qualified `datafusion.public.sales`), so SELECTs return stale data after a write
   (read-after-write correctness break for dbt/BI). *(Verified key mismatch.)*
8. **DEP-01 / DEP-02 (high)** — the recommended Helm deploy is configured to die under load two
   ways: `readOnlyRootFilesystem` blocks spill to `/tmp`, and the coordinator's 8GB engine memory
   budget sits inside a 2Gi pod (OOMKill before spill).

## Cross-cutting themes

- **New subsystems shipped without the hardening the old ones got.** TLS, auth gating, and rate
  limiting exist on the Flight SQL path but are missing on the Quack endpoint (QUACK-05/08), the
  worker Flight service (QUACK-06/07), and the web UI (WEB-01/05).
- **Untrusted-input parsing is the biggest reliability gap.** The Quack wire decoder (QUACK-01..04,
  09..12) and the SQL/expression path (SQL-01) can be driven to panics or process aborts. The
  workspace uses unwind, so most are request-kills, but recursion and allocation aborts are not.
- **Silent-correctness bugs.** PLAN-01 (wrong join column) and COORD-01 (stale cache after write)
  both return *wrong but plausible* results with no error. the hardest class for users to detect.
- **Sanitization built for one sink doesn't cover the new ones.** Error sanitization and PII
  redaction protect the SQL client and audit log, but the new lineage emitter ships raw errors
  (SQL-06, regression) and under-redacted SQL (SQL-07); the web UI exposes raw SQL (WEB-02); and
  several secret fields bypass `SecretString` Debug redaction (CORE-01, regression).
- **Defaults skew insecure/fragile for the reveal.** web_ui on + 0.0.0.0 (WEB-01), Helm `latest`
  tag (DEP-08), memory/spill misconfig (DEP-01/02), placeholder secret shaped like a real one (DEP-10).

## Coverage

Fully reviewed this pass: `sqe-auth`, `sqe-policy`, `sqe-catalog`, `sqe-coordinator` (web/flight +
scheduler/session/write/distributed), `sqe-quack-wire`/`-server`/`-client`, `sqe-worker`, `sqe-sql`,
`sqe-trino-functions`, `sqe-trino-compat`, `sqe-lineage`, `sqe-planner`, `sqe-core`. Light pass:
`sqe-cli`, `sqe-metrics` (both verified clean for scope. CLI-01/MET-01). Dependencies/build/CI/Helm
covered cross-cutting (DEP-*).

**Not deeply reviewed:** `sqe-bench` (benchmark harness, not in the request/data path) and the
vendored `vendor/iceberg-rust/` fork internals (only its supply-chain pinning, DEP-05/06). `cargo deny`
bans/licenses could not run offline (no registry); DEP-06/07 were derived from `Cargo.lock` directly.

## Non-findings (verified safe)

- **WEB-06** — dashboard XSS escaping (`esc()`) is applied at every engine-data sink; verified safe.
- **MET-01** — metric label cardinality is bounded to enum names; no Prometheus blow-up vector.
- **CLI-01** — CLI panics are test-only, guarded, or local-stdin; no remote blast radius.
- The MR !190 TVF guard on the `read_*` functions is intact; CAT-01 is a *parallel* path, not a
  regression of !190.
- `cargo audit` is clean (only the allowed `paste` unmaintained warning).
