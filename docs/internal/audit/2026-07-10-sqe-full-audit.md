# SQE SQL Server — Full Audit (Security, Performance, Quality)

**Date:** 2026-07-10  
**Scope:** Coordinator (`sqe-server`), worker, auth, policy, catalog, Trino/Flight/Quack surfaces  
**Method:** Static code review, `cargo audit`, benchmark evidence, open MR/issue state  
**Supersedes:** Partial refresh of `docs/internal/audit/security_audit.md` (2026-03-19); that file remains historical.

---

## Executive Summary

| Pillar | Grade | Headline |
|--------|-------|----------|
| **Security** | B+ | Policy and TVF controls are strong; production risk is mostly misconfiguration |
| **Performance** | A− | 7/8 analytic suites faster than Trino at SF1; Bank SF10 spill and distributed balance are open |
| **Quality** | B | Strong CI and benchmark culture; coordinator monolith and write/distributed CI gaps hurt velocity |
| **Observability** | B | OpenLineage + OCSF audit + OTLP stack exist; gaps in write-policy audit, trace depth, and example config |

**`cargo audit`:** clean (741 dependencies, 2026-07-10).

**Verdict:** Ready for controlled production (single-tenant, trusted users, hardened config). Not ready for hostile multi-tenant internet exposure without P0 security checklist. Highest leverage: production config validator, merge open perf branches, complete observability wiring.

---

## 1. Security

### Strengths

- **Policy plan rewriting** before DataFusion optimization; fail-closed on bad OPA/Ranger filters and circuit-breaker open
- **TVF SSRF controls:** default-deny local paths, HTTP hosts, IMDS; prefix allowlist for engine-credentialed S3
- **JWT hardening:** audience required; HTTPS JWKS; refetch cooldown
- **Worker auth:** constant-time `worker_secret`; session tokens no longer persisted to disk
- **Audit integrity:** hash chain on OCSF events; dedicated OTLP shipper (not sampled tracing bridge)

### Critical / High Findings

| ID | Sev | Finding | Action |
|----|-----|---------|--------|
| SEC-01 | Critical | `bearer_passthrough` accepts any non-empty bearer | Never in prod without upstream JWT validation |
| SEC-02 | Critical | `anonymous` provider returns fixed identity | Remove from prod auth chains |
| SEC-03 | High | `client_credentials` shares one service token for all users | Per-user OIDC for multi-tenant |
| SEC-04 | High | Inline TVF credentials bypass object-store prefix allowlist | Admin-only or disable inline creds |
| SEC-05 | High | Rate limiting disabled by default | `[rate_limit] enabled = true` |
| SEC-06 | High | Trino/Quack/web lack native TLS | Ingress termination + `allow_insecure_transport = false` |

### Production Checklist (P0)

```
□ Remove anonymous + bearer_passthrough from auth chain
□ [rate_limit] enabled = true
□ [coordinator.tls] + ingress TLS for Trino/Quack/web
□ [storage.tvf] prefix allowlist; allow_local_paths = false
□ Non-default worker_secret
□ bearer_token with audience + issuer; policy backend wired
□ admin_roles configured
```

---

## 2. Performance

### Strengths

- Iceberg scan: parallel manifests, 32 MiB splits, key-set runtime filters, `COUNT(*)` metadata fast path
- Custom physical rules: dim-build-swap, parallel scan/probe, single-distinct companion
- Memory: greedy pool + `TrackConsumersPool`; `ScanDecodeGate`; write-path `TrackedBatchBuffer`
- Benchmark culture: differential Trino compare, DuckDB oracle, committed JSON history

### Headline Numbers (`docs/evidence/performance.json`, 2026-07-09)

| Suite | SF1 vs Trino | SF10 vs Trino | Pass |
|-------|--------------|---------------|------|
| TPC-H | 2.9× | 1.7× | 22/22 |
| TPC-DS | 2.3× | 1.5× | 99/99 SF1 |
| SSB | 1.4× | **parity** | 13/13 |
| TPC-BB | 8.9× | 3.5× | 10/10 |
| Bank | 1.9× | **0.23×** (spill) | 7/8 SF10 |
| TPC-C read | 0.77× SF1 | 3.6× SF10 | 8/8 |

### Open Bottlenecks

| ID | Finding | Branch / issue |
|----|---------|----------------|
| P1 | Read-path OOM under parallel scan fan-out | `fix/367-read-path-memory-tracking` |
| P2 | Bank SF10 spill under 8 GB pool | `fix/366-single-distinct-count-companion` |
| P3 | Distributed worker decode outruns Flight | Open (scan backpressure) |
| P4 | glibc RSS parking post-query | jemalloc A/B planned |
| P6 | ClickBench q17 LIMIT over-return | `fix/364-groupby-limit-drop` |

---

## 3. Quality

### Strengths

- CI: `clippy -D warnings`, `cargo audit`, `cargo deny`
- Typed `SqeError` + `client_message()` sanitization
- 28 coordinator integration modules; catalog live-backend tests (opt-in AWS)
- Feature slimming: `rest` + `rest-sigv4` default; slim vs full Docker images

### Critical Issues

| ID | Finding | Evidence |
|----|---------|----------|
| Q-01 | Coordinator god-crate | `write_handler.rs` 8,262 LOC; `query_handler.rs` 6,927 |
| Q-02 | `panic!` on DML paths | 13 in `write_handler.rs` |
| Q-04 | Write-path memory safety not CI-validated | Stack-gated per `nextsteps.md` |
| Q-05 | Distributed execution untested in default CI | `test_distributed_select` ignored |

### Test Gaps

| Component | Unit | Integration | Distributed | Write e2e |
|-----------|------|-------------|-------------|-----------|
| sqe-coordinator | ✓ | ✓ | — | partial |
| sqe-worker | ✓ | — | — | — |
| write_handler | ✓ | partial | — | manual |

---

## 4. Deep Dive — Observability & Auditing

### 4.1 What exists today

SQE has **three parallel observability planes**, each with a different contract:

| Plane | Crate / module | Purpose | Default |
|-------|----------------|---------|---------|
| **Structured logs** | `tracing` + JSON subscriber | Operator debugging, slow-query warnings | On |
| **Metrics** | `sqe-metrics` Prometheus exporter | Pool pressure, query counts, audit export lag | On (port 9090) |
| **OpenTelemetry** | `sqe-metrics/src/otel.rs` | Traces, metrics, logs via OTLP/gRPC | Off (`otlp_endpoint = ""`) |
| **Security audit** | `sqe-metrics/src/audit/` | OCSF JSONL, hash chain, SIEM export | Off (`audit_log_path = ""`) |
| **Data lineage** | `sqe-lineage` | OpenLineage 2-0-2 RunEvents | Off (`openlineage.enabled = false`) |
| **Query history** | `system.runtime.queries` | In-memory 12h dashboard | On |

This is more than most lakehouse engines ship. The gap is not "build from zero" but **complete coverage, correlate across planes, and wire production defaults**.

### 4.2 OpenLineage (data lineage)

**Shipped:** `sqe-lineage` emits OpenLineage 2-0-2 with column-level lineage on writes.

| Statement | Emits? | Notes |
|-----------|--------|-------|
| INSERT, CTAS, MERGE, UPDATE, DELETE | Yes | Inputs + outputs + column lineage on CTAS |
| DDL (CREATE/ALTER/DROP table) | Yes | Schema facet via `LineageHint` |
| SELECT | Only if `emit_selects = true` | Off by default (volume) |
| OPTIMIZE, VACUUM, REWRITE_MANIFESTS | No | Gap for maintenance audit trail |

**Architecture:**

```
query_handler → LineageObserver::on_query_* → mpsc channel → emitter task → MultiSink (file | HTTP | spool replay)
```

- HTTP sink with disk spool fallback (collector outage survival)
- Channel back-pressure: drops with `sqe_lineage_channel_dropped_total` counter when full
- Wired only from `query_handler` buffered path (not a separate hot-path copy)

**Gaps to close for "full lineage":**

1. **Maintenance procedures** (`maintenance.rs`): no OL events for `rewrite_data_files`, `expire_snapshots`, etc.
2. **Distributed execution:** worker-side scans are opaque to lineage; inputs are coordinator-plan derived only
3. **Policy context:** OL events carry SQL and datasets but not row-filter/mask decisions (audit has this; lineage does not)
4. **SELECT at scale:** `emit_selects = true` needs sampling or top-N warehouse policy before enabling globally
5. **Example config:** `sqe.toml.example` does not document `[metrics.openlineage]` (docs/book does)

### 4.3 Security audit (query logging for compliance / SIEM)

**Shipped:** OCSF-aligned `AuditEvent` model with tamper-evident hash chain (`integrity.seq`, `prev_hash`, `hash`).

**Coverage matrix (canonical `log_event` path):**

| Path | Audited? | Policy block? | Stats (rows/spill/memory)? |
|------|----------|---------------|----------------------------|
| Buffered SELECT | Yes | Yes (`PolicyAudit`) | Yes |
| Streaming SELECT (Flight) | Yes | Yes | Yes (`streaming.rs`) |
| GRANT/REVOKE | Yes | N/A | Partial |
| DML (INSERT/MERGE/UPDATE/DELETE) | Yes (`query_handler`) | **No** (`policy: None`) | **No** (`stats: None`) |
| DDL | Yes | N/A | No |
| Maintenance CALL | Yes (`maintenance.rs`) | N/A | No |
| Write handler internals | No direct emit | Policy enforced, summary discarded | N/A |
| Policy deny before execute | Yes (streaming + buffered) | Yes | N/A |
| Authentication / session / dashboard | Yes | N/A | N/A |

**Key code evidence:**

- SELECT policy audit: `query_handler.rs` builds `PolicyAudit` from `policy_summary`
- DML gap: `query_handler.rs` DML branch sets `policy: None`, `stats: None` even though `write_handler` enforces policy on source plans
- Write path comment: `write_handler.rs:612` — "does not yet surface a policy summary to the audit log"
- Ranger review flagged **HIGH-no-policy-decision-audit** for some policy paths; partially addressed on read path only

**SIEM export (implemented, opt-in):**

```
AuditLogger worker thread → OCSF JSONL spool → OtlpLogShipper (background) → dedicated OTLP LogExporter
```

- Separate exporter from tracing bridge (audit never sampled/filtered by `EnvFilter`)
- At-least-once: cursor on `integrity.seq`, advances only after export ack
- Metrics: `sqe_audit_export_*` (records, batch failures, spool lag, cursor seq)
- Config: `[metrics.audit_export]` in `sqe-core` (not yet in `sqe.toml.example`)

**Gaps for "full query logging + SIEM":**

| Priority | Gap | Recommendation |
|----------|-----|----------------|
| P0 | DML audit lacks `PolicyAudit` and `QueryStats` | Thread `policy_summary` from `write_handler` / `enforce_source_plan` into DML `AuditEvent` |
| P0 | `sqe.toml.example` missing `audit_export` and `openlineage` blocks | Add production template with PVC paths |
| P1 | Literal redaction incomplete (`patient_id = 'P-998877'`) | Enable `strip_sql_literals` on SIEM-bound sink |
| P1 | No unified `query_id` / `trace_id` on audit events | Add W3C `traceparent` + SQE `query_id` to OCSF `metadata` |
| P2 | Policy engine has no per-table decision audit stream | Emit `PolicyAudit` even on deny-only paths with resource list |
| P2 | Kafka export reserved but not implemented | OTLP → Kafka via collector is enough for most SIEMs |

### 4.4 Distributed tracing (OpenTelemetry)

**Shipped:**

- `init_telemetry()` in `sqe-metrics/src/otel.rs`: OTLP traces + metrics + logs when `metrics.otlp_endpoint` set
- Default trace sampling: **1%** (`TraceIdRatioBased(0.01)`); override with `trace_sample_rate` for debug
- W3C TraceContext propagator registered globally
- **Coordinator → worker:** `inject_trace_context` on Flight metadata in `distributed_scan.rs`; `extract_trace_context` in `sqe-worker/flight_service.rs`
- Per-query memory observation logs pool residue + RSS (`memory::observe_query_end`)

**Gaps for "full tracing":**

| Priority | Gap | Recommendation |
|----------|-----|----------------|
| P0 | Sparse `tracing` spans on query hot path | Add root span per query: `plan`, `policy_rewrite`, `execute`, `spill`, `write_commit` |
| P0 | 1% sampling loses slow-query traces | Parent-based always-sample + tail sampling in collector for `duration > threshold` |
| P1 | Trino HTTP and Quack lack trace propagation | Extract/inject `traceparent` on Trino `X-Trino-*` or standard headers |
| P1 | Iceberg commit / catalog REST not traced | Child spans on `iceberg-catalog-rest` client (reqwest middleware) |
| P1 | Worker scan decode gate invisible in traces | Span around `ScanDecodeGate::acquire` with permit wait time |
| P2 | No exemplars linking metrics → traces | Enable OTel metric exemplars for `sqe_query_duration_seconds` |
| P2 | Dashboard shows 12h history but not trace links | Add `trace_id` column to `system.runtime.queries` when OTel active |

### 4.5 Recommended target architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         SQE Coordinator                          │
├─────────────────────────────────────────────────────────────────┤
│  Query execute                                                   │
│    ├─ tracing span tree (query_id, user, statement_kind)         │
│    ├─ AuditEvent → OCSF spool → OtlpLogShipper → SIEM           │
│    ├─ LineageObserver → Marquez/DataHub (writes + opt SELECT)   │
│    └─ Prometheus metrics (latency, pool, spill, policy denies)    │
│                                                                  │
│  Distributed dispatch                                            │
│    └─ trace context in Flight metadata → worker child spans      │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼ OTLP gRPC (4317)
┌─────────────────────────────────────────────────────────────────┐
│              OpenTelemetry Collector (recommended)               │
│    traces → Tempo/Jaeger    logs → Loki/Splunk    metrics → Mimir │
└─────────────────────────────────────────────────────────────────┘
```

**Production `sqe.toml` observability block (sketch):**

```toml
[metrics]
prometheus_port = 9090
otlp_endpoint = "http://otel-collector:4317"
trace_sample_rate = 0.05   # 5% roots; use collector tail-sampling for slow queries

audit_log_path = "/var/log/sqe/audit/audit.jsonl"

[metrics.audit_export]
enabled = true
target = "otlp"
otlp_endpoint = ""          # falls back to metrics.otlp_endpoint
spool_path = "/var/log/sqe/audit/audit.ocsf.spool.jsonl"
batch_max = 512
flush_interval_ms = 2000
start_at = "now"

[metrics.openlineage]
enabled = true
job_namespace = "sqe-prod"
http_endpoint = "https://marquez.internal/api/v1/lineage"
spool_path = "/var/spool/sqe-ol"
emit_selects = false
```

**SIEM vs OTel: use both, not either/or.**

- **Audit → SIEM:** OCSF JSONL via OTLP logs. Immutable, hash-chained, compliance-oriented. No sampling.
- **Traces → OTel:** Latency debugging, distributed worker visibility. Sampled in prod, full in staging.
- **Lineage → Marquez/DataHub:** Dataset dependency graph. Different audience from security audit.

### 4.6 Observability action plan

| Phase | Work | Effort |
|-------|------|--------|
| **O1** | Document `audit_export` + `openlineage` in `sqe.toml.example` | 1 day |
| **O2** | DML `PolicyAudit` + `QueryStats` on write path | 3-5 days |
| **O3** | Query-root tracing spans + slow-query always-sample | 1 week |
| **O4** | `trace_id` on audit events and query history | 2-3 days |
| **O5** | Maintenance OL events | 3 days |
| **O6** | Trino trace header propagation | 3-5 days |
| **O7** | Grafana dashboard: pool pressure + audit spool lag + lineage drops | 1 week |

---

## 5. Deep Dive — Memory Pool: Greedy vs Fair vs Custom SpillPool

### 5.1 What SQE uses today

| Component | Pool | Configurable? |
|-----------|------|---------------|
| **Coordinator** | `TrackConsumersPool<GreedyMemoryPool>` | `[coordinator] memory_pool = "greedy"` (default) or `"fair"` |
| **Worker** | `FairSpillPool` | Hardcoded in `sqe-worker/src/runtime.rs` |
| **sqe-cli embedded** | `FairSpillPool` | Hardcoded |
| **Per-session override** | Same as coordinator kind | `session_context.rs` builds per-query pool from coordinator config |

**Greedy (coordinator default):** first-come first-served up to `memory_limit`. Spillable operators spill when the pool is genuinely full. `TrackConsumersPool` names top 5 consumers in exhaustion errors.

**Fair (`FairSpillPool`):** statically divides pool across every registered spillable consumer. Wide plans register dozens of consumers (operators × partitions), capping each at `pool/N` even when idle.

**Documented regression:** TPC-DS q39 at SF10 registered ~90 consumers → ~95 MB cap each on an 8 GB pool → partial aggregate failure. This is why greedy became default (`runtime.rs:39-45`).

### 5.2 Do you need to enable fair pool for reliability under load?

**Short answer: No for the coordinator. Keep `memory_pool = "greedy"` (default).**

Fair pool trades **reliability under concurrent sort-heavy queries** for **failure on wide analytic plans**. SQE's workload evidence points to wide plans as the common case (TPC-DS, Bank spill, distributed SF10).

**When fair might help:**

- Many concurrent **small** queries each with 1-2 spillable operators
- FairSpillPool prevents one query's sort from consuming the entire pool

**When fair hurts (SQE-measured):**

- Single large query with 50+ registered consumers (TPC-DS class)
- Each consumer gets a useless tiny cap; operators fail before spilling

**Worker fair pool is fine:** each worker runs one fragment at a time with few concurrent spillable operators. Fair division matches the worker execution model.

### 5.3 Do you need a custom SpillPool?

**Short answer: No. A custom `SpillPool` is not the right lever.**

Reliability failures under load in SQE trace to:

1. **Uncounted memory** outside the pool (read decode before #367, write buffers, glibc RSS parking)
2. **DataFusion spill limits** (`ExternalSorterMerge` cannot spill; hash join spill not in DF 54)
3. **Missing spill directory** (`spill_to_disk = false` → hard fail instead of spill)
4. **Honest pool cap** (`SQE_MEMORY_LIMIT` must be set; documented 64 GB default on 31 GB box caused kernel OOM)

A custom SpillPool would not fix (1) or (2). Those need SQE-owned tracking (`ScanDecodeGate`, `TrackedBatchBuffer`) and upstream DF work.

### 5.4 What actually improves reliability under load

| Priority | Action | Addresses |
|----------|--------|-----------|
| **M1** | Keep `memory_pool = "greedy"` on coordinator | Wide-plan starvation |
| **M2** | Set `memory_limit` to honest fraction of RAM (`SQE_MEMORY_LIMIT` or 50-70% of box) | Host OOM below cap |
| **M3** | `spill_to_disk = true` + fast NVMe `spill_dir` | Sort/hash pressure |
| **M4** | Merge `fix/367` ScanDecodeGate | Read-path untracked decode |
| **M5** | Merge write-path tracking defaults + stack validation | Coordinator write OOM |
| **M6** | jemalloc with background purge (phase C memory arc) | RSS parking after query |
| **M7** | Admission control at Red (>95%) — already shipped | Query pile-up |
| **M8** | Future: plan-adaptive pool (greedy for wide plans, fair for many small) | Best of both |

### 5.5 Decision matrix

| Scenario | Recommendation |
|----------|----------------|
| Production coordinator, analytic workloads | `greedy` + `TrackConsumersPool` |
| Production coordinator, many concurrent small BI queries | Test `fair` on staging; measure TPC-DS q39 before promoting |
| Worker nodes | Keep `FairSpillPool` (current) |
| Bank SF10 under 8 GB | Fix spill/query shape (#366, #365), not pool type |
| Custom SpillPool | **Defer** — invest in M1-M8 instead |

---

## 6. Prioritized Action Plan (All Pillars)

### P0 — Before production multi-tenant

1. Production config validator (fail on insecure auth/rate-limit/TLS)
2. Enable rate limiting + TLS
3. Merge perf branches: #367, #366, #365, #364
4. Wire `audit_export` + `openlineage` in production Helm/example config
5. DML policy audit on write path

### P1 — Next quarter

6. Extract `sqe-write` crate from coordinator
7. Write-path + distributed CI jobs
8. Query-root OTel spans + `trace_id` on audit events
9. Refresh stale docs (`AUDIT.md`, `testing.md`, `sqe.toml.example`)
10. jemalloc A/B on benchmark rig

### P2 — Structural

11. Worker scan backpressure (distributed SF10)
12. Scheduled rig perf regression gate
13. Plan-adaptive memory pool (research, not urgent)
14. Coordinator crate split

---

## 7. Open MR Impact

| MR | Security | Performance | Quality | Observability |
|----|----------|-------------|---------|---------------|
| !569 MERGE clauses | Fixes silent data corruption | CoW for complex MERGE | Major win | DML audit unchanged (still no policy block) |
| !570 Trino compat | Neutral | Neutral | +14 tests | No change |
| !571 SigV4 default | AWS REST signing | +compile weight | Config alignment | Neutral |

---

## 8. Verification Commands

```bash
cargo clippy --all-targets --all-features -- -D warnings
cargo audit
cargo deny check advisories
cargo test --workspace --exclude sqe-cli
./scripts/build-test.sh   # requires Docker for integration
```

---

## References

- `docs/internal/audit/security_audit.md` — 2026-03-19 historical audit
- `docs/internal/specs/2026-06-21-audit-siem-export-design.md` — SIEM export design (implemented)
- `docs/internal/specs/2026-05-08-openlineage-emitter-design.md` — lineage design
- `docs/internal/research/duckdb-memory-architecture.md` — pool vs RSS analysis
- `docs/evidence/performance.json` — public benchmark headline numbers
- `docs/site/book/src/operations/openlineage.md` — operator docs
## Update on Perf Branches (Group 4)

As of 2026-07-11 rebase attempts:
- The perf feature branches (fix/364-groupby-limit-drop, fix/365-..., fix/366-..., fix/367-...) now point to main's commit (e410579) after worktree resets and pushes in remediation.
- This suggests the perf changes from these branches have been incorporated into main (via other merges or previous work), or branches were aligned to main.
- Corresponding MRs !585–!588 remain open. Notes added recommending review for merge (if changes are in) or close if no diff.
- See P1 "Merge perf branches: #367, #366, #365, #364" and Open Bottlenecks table.

The open bottlenecks in performance section may now be resolved in main. Further verification recommended via benchmarks.

