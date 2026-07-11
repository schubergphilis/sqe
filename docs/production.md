# SQE Production Guide

How to run SQE in production when you start small but expect to scale data volume, query concurrency, and team count significantly. The goal is one honest configuration path you extend over time, not a throwaway dev stack you replace at scale.

For the full security, performance, quality, and observability audit, see [`docs/internal/audit/2026-07-10-sqe-full-audit.md`](internal/audit/2026-07-10-sqe-full-audit.md).

---

## Principles

1. **Coordinator stays on greedy memory pool.** Fair pool starves wide analytic plans at scale.
2. **Set an honest `memory_limit`.** Use 40-50% of allocatable RAM per process, not the config default on a small box.
3. **Spill must be on** with fast local disk before you push SF10-class workloads.
4. **Security and audit from day one** if compliance or multi-team access matters. Retroactive audit history is not recoverable.
5. **Add workers when scan CPU saturates**, not before you have evidence single-node is the bottleneck.

---

## Phase 0: Day one (single coordinator)

### Topology

- One `sqe-coordinator` or `sqe-server` (all-in-one). No workers.
- Slim Docker image (`Dockerfile`) with REST catalog + SigV4 (`rest` + `rest-sigv4`). Use `Dockerfile.full` only when runtime config must dispatch to Glue, HMS, or other SDK backends.

### Security (required even at small scale)

```toml
[auth]
# Use real OIDC bearer_token validation.
# Do NOT use: anonymous, bearer_passthrough, or client_credentials as the only provider.

[rate_limit]
enabled = true

[coordinator.tls]
# Enable Flight SQL TLS, or terminate TLS at ingress.

[storage.tvf]
allow_local_paths = false
allow_http = false
allowed_object_store_prefixes = ["s3://your-warehouse/"]
```

### Memory

```toml
[coordinator]
memory_pool = "greedy"        # default; do not switch to fair on the coordinator
memory_limit = "12GB"         # tune to ~40-50% of box RAM (example for a 32 GB host)
spill_to_disk = true
spill_dir = "/fast-nvme/sqe-spill"
```

Greedy pool lets one large operator use the full budget until spill. Fair pool divides memory across every registered spillable consumer. Wide plans (TPC-DS class) register dozens of consumers and fail before spilling when fair is enabled. Workers use fair pool internally; that asymmetry is intentional.

### Observability

Enable durable audit and lineage early if governance matters. Do not write audit logs to `/tmp` or container `emptyDir`.

```toml
[metrics]
prometheus_port = 9090
otlp_endpoint = "http://otel-collector:4317"
trace_sample_rate = 0.10      # 10% at low QPS; lower to 0.01-0.05 at high QPS

audit_log_path = "/var/log/sqe/audit/audit.jsonl"   # must survive pod restart (PVC)

[metrics.audit_export]
enabled = true
target = "otlp"
# OCSF audit ships to SIEM via OTLP logs; uses metrics.otlp_endpoint when otlp_endpoint is empty

[metrics.openlineage]
enabled = true
http_endpoint = "https://marquez.example.com/api/v1/lineage"
spool_path = "/var/spool/sqe-ol"
emit_selects = false            # enable only when read lineage is required
```

See [`docs/site/book/src/operations/openlineage.md`](site/book/src/operations/openlineage.md) for sink options and Marquez setup.

### Catalog

- Polaris or Nessie over Iceberg REST.
- SigV4 in the default build covers AWS Iceberg REST (Glue/S3 Tables federated endpoints) without the full Glue SDK image.
- Defer `full-backends` until runtime config actually dispatches to Glue, HMS, or JDBC SQL catalogs.

---

## Phase 1: Growth (roughly 10x data, add workers)

### When to add workers

Add `sqe-worker` replicas when:

- Coordinator CPU is saturated during scans while network and object storage still have headroom.
- Single-node benchmarks show a scan throughput ceiling, not memory exhaustion.
- Large queries hit `memory_limit` with spill enabled and still need more parallel decode.

### Topology

```
                    Ingress (TLS)
                         |
              +----------+----------+
              |   sqe-coordinator   |  planning, auth, policy, audit
              +----------+----------+
                         |  Arrow Flight + signed scan tickets
         +---------------+---------------+
         v               v               v
    sqe-worker      sqe-worker      sqe-worker
```

Coordinator: **greedy** pool. Workers: **fair** pool (hardcoded in `sqe-worker`).

### Sizing

| Component | Starting point | Scale-up signal |
|-----------|----------------|-----------------|
| Coordinator | 8 vCPU, 32 GB RAM | Planning latency, auth/policy CPU |
| Worker | 8 vCPU, 32 GB RAM each | Scan/decode CPU, worker OOM |
| Spill volume | 2x expected sort working set | Spill stalls, long-running aggregations |
| `memory_limit` | 40-50% of pod RAM | `ResourceExhausted` vs host OOM kill |

Set `SQE_MEMORY_LIMIT` in Kubernetes so the process cap matches the pod limit. A documented 64 GB default on a 31 GB node caused kernel OOM in benchmark runs.

### Branches to merge before heavy SF10 load

These are reliability fixes, not optional optimizations:

| Branch / issue | What it fixes |
|----------------|---------------|
| `fix/367-read-path-memory-tracking` | Read decode fan-out OOM under parallel scan |
| `fix/366-single-distinct-count-companion` | Bank-class mixed `COUNT(DISTINCT)` spill |
| `fix/365-idle-timeout-operator-progress` | False abort on long spilling queries |
| `fix/364-groupby-limit-drop` | `GROUP BY ... LIMIT` over-return (ClickBench q17) |

---

## Phase 2: Large scale (100x data, multi-team, SF10+)

### Architecture

- **Distributed execution by default** for large table scans.
- **Warehouse on fast object storage** with a fair SQE-vs-Trino comparison path (same endpoint for both engines).
- **Polaris with Postgres persistence.** In-memory Polaris loses warehouse metadata on stack restart.
- **Coordinator HA** (multiple coordinators behind a load balancer) only after you understand session stickiness: sessions are coordinator-local today.

### Security hardening

- Per-user OIDC. No shared service token for all users.
- Ranger or OPA policy backend (`policy.engine` not `passthrough`).
- `mask_key` configured for hash column masks.
- Inline-credential TVFs disabled or restricted to admin roles.
- Restrict catalog/execution error detail for untrusted clients if needed.

### Observability at scale

- OpenTelemetry collector with **tail sampling**: always retain slow and failed traces; sample the rest.
- Alert on:
  - Memory pressure in Red band (`>95%` pool utilization)
  - `sqe_audit_export_spool_lag_bytes` growing (SIEM backpressure)
  - `sqe_lineage_channel_dropped_total` > 0 (lineage overload)
- Close the DML audit gap: write-path `PolicyAudit` and `QueryStats` on INSERT/MERGE/UPDATE/DELETE (tracked in audit doc).

### Performance tuning order

1. Honest `memory_limit` + `spill_to_disk` on NVMe
2. Add workers (2, then 4, then N)
3. Enable `parallel_probe_scan` only after memory clamp is validated (TPC-DS regresses without clamp)
4. jemalloc A/B for glibc RSS parking after large sorts
5. Do **not** move coordinator to `memory_pool = "fair"`

---

## Production baseline (`sqe.toml`)

Copy and tune per environment:

```toml
[coordinator]
memory_pool = "greedy"
memory_limit = "24GB"          # 40-50% of pod allocatable RAM
spill_to_disk = true
spill_dir = "/var/sqe/spill"

[worker]
memory_limit = "24GB"
spill_to_disk = true
spill_dir = "/var/sqe/spill"

[rate_limit]
enabled = true

[metrics]
prometheus_port = 9090
otlp_endpoint = "http://otel-collector.observability:4317"
trace_sample_rate = 0.05

audit_log_path = "/var/log/sqe/audit/audit.jsonl"

[metrics.audit_export]
enabled = true
target = "otlp"

[metrics.openlineage]
enabled = true
emit_selects = false
# Set http_endpoint and/or file_path; use spool_path when HTTP is configured.

[storage.tvf]
allow_local_paths = false
allow_http = false
allowed_object_store_prefixes = ["s3://prod-warehouse/"]
```

Environment overrides follow the usual `SQE_*` / `SQE_METRICS__*` conventions (see [`sqe.toml.example`](../sqe.toml.example) and [`docs/site/book/src/deployment/configuration.md`](site/book/src/deployment/configuration.md)).

---

## Memory pool decision

| Question | Answer |
|----------|--------|
| Coordinator: greedy or fair? | **Greedy** (default). Fair only after staging proof on your workload mix. |
| Worker pool? | **Fair** (current default in `sqe-worker`). No change needed. |
| Custom SpillPool? | **No.** Fix pool cap honesty, spill directory, read/write memory tracking, and upstream DataFusion spill limits instead. |
| Bank SF10 under 8 GB pool? | Query-shape and spill fixes (#366, #365), not pool type. |

---

## Common scale-up mistakes

| Mistake | Why it hurts |
|---------|--------------|
| `memory_pool = "fair"` on coordinator | Wide plans cap each consumer at `pool/N` and fail before spill |
| `spill_to_disk = false` | Sorts and joins error instead of spilling |
| `bearer_passthrough` or `anonymous` auth | Per-user policy breaks when you add tenants |
| `full-backends` image without need | Larger binary, slower builds, wider attack surface |
| Audit on ephemeral storage | Audit trail lost on restart |
| `emit_selects = true` globally | Lineage and audit volume explodes at high QPS |
| Custom SpillPool | Wrong lever; does not fix untracked decode or DF merge spill limits |

---

## Observability planes (what to use when)

| Plane | Use for | Sampling |
|-------|---------|----------|
| OCSF audit + `audit_export` | Compliance, SIEM, who-ran-what | Never sample |
| OpenLineage | Dataset dependency graph (Marquez/DataHub) | N/A |
| OpenTelemetry traces | Latency debugging, distributed worker visibility | Sample in prod; tail-sample slow/failed |
| Prometheus metrics | SLOs, pool pressure, query rates | N/A |

Route audit logs and traces through an OpenTelemetry Collector when possible. Audit uses a dedicated OTLP log exporter so records are not dropped by trace sampling or log filters.

---

## Checklist summary

### Before first production traffic

- [ ] Real OIDC auth (no anonymous / passthrough)
- [ ] Rate limiting enabled
- [ ] TLS on Flight or at ingress
- [ ] TVF prefix allowlist; local paths and HTTP off
- [ ] `memory_pool = greedy`, honest `memory_limit`, spill on fast disk
- [ ] Audit log on persistent volume
- [ ] `audit_export` and `openlineage` configured if governance required

### Before SF10-class load

- [ ] Distributed workers if scan CPU bound
- [ ] Memory fixes merged (#367, #366, #365, #364)
- [ ] `SQE_MEMORY_LIMIT` matches pod resources
- [ ] Polaris (or catalog) backed by durable storage
- [ ] OTel alerts on memory Red and audit spool lag

### Before multi-tenant

- [ ] Per-user identity end to end
- [ ] Policy backend (Ranger/OPA) with `mask_key`
- [ ] DML policy fields in audit (when implemented)
- [ ] Inline TVF credentials restricted

---

## Related docs

- [Deployment configuration](site/book/src/deployment/configuration.md)
- [OpenLineage operations](site/book/src/operations/openlineage.md)
- [Full audit (2026-07-10)](internal/audit/2026-07-10-sqe-full-audit.md)
- [Example config](../sqe.toml.example)
- [QUICKSTART](../QUICKSTART.md) for local validation before production cutover
## Recent Audit Remediation Notes (2026-07-11)

- `[rate_limit]` section and full production examples now documented in `sqe.toml.example` (see MRs !590, !591).
- Stale `AUDIT.md` and the full `2026-07-10-sqe-full-audit.md` refreshed with progress (MRs !592, !594).
- Jemalloc and memory recommendations added to example (MR !593).
- Babysat fixes for several R items verified and pushed (!579–!581).
- Perf Group 4 branches currently at main; MRs !585–!588 have notes.
- Production config validator, DML audit, observability wiring, and CI for write/distributed have seen substantial progress via prior and current MRs.

Refer to the full audit for the prioritized plan. Keep `sqe.toml.example` and this guide in sync as more items land.

