# Audit Issue Groups and Worktree Plan

**Source:** [`2026-07-10-sqe-full-audit.md`](2026-07-10-sqe-full-audit.md)  
**Date:** 2026-07-10

Issues are grouped by theme. Each group gets an isolated git worktree under `.worktrees/` and a feature branch. Merge order is bottom-up where dependencies exist.

---

## Group 0: Documentation (MR first)

| ID | Issue | Branch | Worktree |
|----|-------|--------|----------|
| DOC-01 | Full audit report | `docs/audit-production-guide` | (main workspace) |
| DOC-02 | Production operator guide | `docs/audit-production-guide` | (main workspace) |

**Deliverable:** `docs/internal/audit/2026-07-10-sqe-full-audit.md`, `docs/production.md`, this file.

---

## Group 1: Observability config (O1)

| ID | Severity | Issue | Branch |
|----|----------|-------|--------|
| O1-01 | Medium | `sqe.toml.example` missing `[metrics.audit_export]` | `fix/observability-config` |
| O1-02 | Medium | `sqe.toml.example` missing `[metrics.openlineage]` | `fix/observability-config` |
| O1-03 | Low | `trace_sample_rate` undocumented in example | `fix/observability-config` |
| O1-04 | Low | README pointer to `docs/production.md` | `fix/observability-config` |

**Worktree:** `.worktrees/fix-observability-config`

---

## Group 2: DML audit completeness (O2)

| ID | Severity | Issue | Branch |
|----|----------|-------|--------|
| O2-01 | High | DML `AuditEvent` has `policy: None` despite write-path enforcement | `fix/dml-audit-policy` |
| O2-02 | High | DML `AuditEvent` has `stats: None` (no rows/spill/memory) | `fix/dml-audit-policy` |
| O2-03 | Medium | `write_handler` discards `policy_summary` from `enforce_source_plan` | `fix/dml-audit-policy` |

**Worktree:** `.worktrees/fix-dml-audit-policy`

**Depends on:** none (code-only).

---

## Group 3: Production config guard (SEC P0)

| ID | Severity | Issue | Branch |
|----|----------|-------|--------|
| SEC-P0-01 | Critical | No fail-fast when insecure auth in production | `fix/prod-config-validator` |
| SEC-P0-02 | High | Rate limit off with no prod warning escalation | `fix/prod-config-validator` |
| SEC-P0-03 | High | `allow_insecure_transport` not blocked in prod mode | `fix/prod-config-validator` |

**Worktree:** `.worktrees/fix-prod-config-validator`

**Design:** `[coordinator] production_mode = true` or `SQE_PRODUCTION_MODE=1` triggers startup validation (errors, not just warns).

---

## Group 4: Reliability merges (existing branches)

| ID | Issue | Branch | Status |
|----|-------|--------|--------|
| P1 | Read-path ScanDecodeGate | `fix/367-read-path-memory-tracking` | Open MR |
| P2 | COUNT(DISTINCT) companion | `fix/366-single-distinct-count-companion` | Open MR |
| P5 | Idle timeout + spill | `fix/365-idle-timeout-operator-progress` | Open MR |
| P6 | GROUP BY LIMIT | `fix/364-groupby-limit-drop` | Open MR |
| !569 | MERGE clauses | `feat/merge-full-clauses-scd2` | Open MR |
| !570 | Trino compat | `fix/trino-compat-347-348-341` | Open MR |

**Action:** Review and merge existing MRs; no new worktree.

---

## Group 5: Quality / structure (deferred)

| ID | Severity | Issue | Notes |
|----|----------|-------|-------|
| Q-01 | Critical | Coordinator god-crate (`write_handler` 8K LOC) | Extract `sqe-write` crate |
| Q-02 | High | 13 `panic!` in `write_handler` | Separate branch after DML audit |
| Q-04 | High | Write-path CI validation | Polaris+RustFS job |
| Q-05 | High | Distributed CI smoke | Un-ignore distributed test |

**Worktree:** deferred to follow-up epic.

---

## Group 6: Tracing depth (O3, deferred)

| ID | Issue | Notes |
|----|-------|-------|
| O3-01 | Query-root tracing spans | `plan`, `policy`, `execute`, `write_commit` |
| O3-02 | `trace_id` on audit events | Correlate SIEM + Tempo |
| O3-03 | Trino HTTP trace propagation | `traceparent` header |

**Worktree:** `feat/query-tracing-spans` (future).

---

## Merge order

```
docs/audit-production-guide     (Group 0)
fix/observability-config        (Group 1)
fix/prod-config-validator       (Group 3)
fix/dml-audit-policy            (Group 2)
+ existing perf/MERGE MRs        (Group 4)
```