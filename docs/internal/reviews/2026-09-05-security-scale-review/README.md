# Independent security and scale review — 2026-09-05

> For the combined backlog of all 109 findings, grouped by area and priority, use the [combined prioritized index](../2026-09-05-combined-prioritized-findings.md). It takes precedence for scheduling; this review retains its original evidence and scope.

Reviewed commit: `c4bc7aff64c82f30feb82684c3339a8ae8cda1e7`.

Seven additional high-severity findings, focused on trust boundaries, result correctness and large-data memory safety. At review time no production code had been changed. **Update 2026-09-05:** all seven are fixed on branch `fix/security-scale-review-2026-09-05`; each finding file ends with a "Resolution" section naming the code and tests. `reproduce.py` now runs the production regression tests for 900 and 903 instead of an extracted copy of the vulnerable code.

## Findings by area and priority

Areas are ordered by recommended implementation sequence. Within each area, findings are ordered by urgency, exposure and implementation effort. Existing issue IDs remain stable.

- **P1 — immediate:** address before a shared production rollout; mitigate existing deployments promptly.
- **P2 — next:** address before large-scale workloads are enabled.

All seven findings retain high severity. Priority describes scheduling, not a downgrade of impact. Rows within the same priority are listed in recommended order.

### 1. External data access and credential isolation

| Order | Priority | ID | Finding | Reason for order |
|---|---|---|---|---|
| 1 | P1 | [900](900-http-allowlist-userinfo-bypass.md) | HTTP endpoint allowlist bypass | Reproduced security-boundary defect with a small, focused fix. |
| 2 | P1 | [901](901-shared-runtime-object-store-credentials.md) | Cross-session S3 store replacement | Shared credential/endpoint state threatens tenant isolation; requires provider/runtime isolation work. |

### 2. Result-cache authorization and correctness

| Order | Priority | ID | Finding | Reason for order |
|---|---|---|---|---|
| 1 | P1 | [902](902-result-cache-policy-revalidation.md) | Cached results bypass updated row filters, masks and denials | Previously permitted data can remain visible after a policy restriction. |
| 2 | P1 | [904](904-result-cache-session-context.md) | Cache identity omits catalog/schema and stable principal identity | Results can cross session contexts; distinct subjects sharing a username add a conditional disclosure risk. |
| 3 | P1 | [903](903-result-cache-sql-literal-collisions.md) | SQL normalization merges semantically different queries | Reproduced wrong-result defect; preserve exact SQL bytes as a focused fix. |

Treat 902–904 as one cache-hardening workstream. Full result-cache disablement is a temporary mitigation for all three, with an expected latency/scan-cost increase. Cache is enabled by default, and small aggregate results over large datasets remain eligible.

### 3. Memory safety and large-data execution

| Order | Priority | ID | Finding | Reason for order |
|---|---|---|---|---|
| 1 | P1 | [906](906-json-decompression-memory.md) | JSON decompression and decoding bypass memory limits | A small permitted compressed object can trigger node-wide resource exhaustion. |
| 2 | P2 | [905](905-partitioned-write-fanout.md) | Partitioned writes retain unbounded writers by default | High partition cardinality and concurrent writes can exhaust memory; fix before scaling ingest/CTAS. |

Promote 905 to P1 if high-cardinality partitioned writes are already running in production. Its remedy must include shared pool accounting, not just a separate budget for each write.

These priorities and mitigations are recommendations; no production configuration or code has been changed.

## Method and limits

Read the previous review index and relevant individual findings, then traced current implementations through session construction, TVF policy checks, object-store registration, result-cache lookup/authorization, streaming writer selection, vendored fanout, and buffered JSON decoding. Current source already contains fixes beyond the earlier review; prior findings were not assumed to remain open. This is a focused independent review, not an exhaustive re-audit of all crates or a cloud well-architected certification.

The findings include exact source references, prerequisites, remediation directions and acceptance criteria. Reproduction evidence is deliberately distinguished from static call-path evidence. No production services were accessed, no live exploit or destructive/OOM test was run, and no throughput benchmark was performed. No dependency-vulnerability audit was performed. The full Cargo test suite was not run for this documentation-only change.

## Offline verification

```sh
python3 docs/internal/reviews/2026-09-05-security-scale-review/reproduce.py
```

Requires Python 3 and `rustc`; no downloaded dependencies. The script extracts the current Rust endpoint method verbatim and the current SQL-normalization expression, compiles a tiny harness, and checks their behavior. Its Python URL parse illustrates destination semantics; it does not exercise the production HTTP client. A bounded gzip example illustrates expansion. The remaining findings are source-traced and have integration/scale tests specified for follow-up.

Output observed at review time, against commit `c4bc7aff`:

```text
CONFIRMED: endpoint validator accepts allowed host in URL username
CONFIRMED: distinct SQL literals collide: SELECT 'secret' / SELECT 'SECRET'
CONFIRMED: distinct SQL literals collide: SELECT 'a  b' / SELECT 'a b'
Standard URL parser destination: blocked.example
Bounded gzip illustration: 8188 compressed bytes -> 8388617 decoded bytes
```

After the fix the script runs the production unit tests for the endpoint gate and the cache key (requires `cargo`) and reports `FIXED` for both.

## Relationship to the September 4 review

Each issue describes its overlap explicitly. 905 expands the earlier review's brief reference to fanout as future work into a concrete default-path and pool-accounting finding; it does not repeat its MoR-vector issue. 902 concerns materialized result reuse, not Ranger's policy-cache key or table-property cache. 901 concerns coordinator registry isolation, not worker ticket credentials. 900 bypasses an existing file-reader destination gate, unlike the earlier ungated Quack TVF. No separate finding is counted for issues already covered such as centralized distributed aggregation, repeated manifest walks, or full Trino result materialization.
