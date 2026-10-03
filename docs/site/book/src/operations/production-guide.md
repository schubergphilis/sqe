# SQE Production Guide

How to run SQE in production when you start small but expect to scale data volume, query concurrency, and team count significantly. The goal is one honest configuration path you extend over time, not a throwaway dev stack you replace at scale.

For the full security, performance, quality, and observability audit, see `docs/internal/audit/2026-07-10-sqe-full-audit.md`.

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
- Slim Docker image (`docker build .`) with the catalog crate's default feature set plus the Glue Hive root, which `sqe-server` always compiles in. The remaining backends (HMS, JDBC, Hadoop): `docker build --build-arg CATALOG_FEATURES=sqe-catalog/full-backends -t sqe-full .`.

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

Keep the global list to what every user may read. A bucket that belongs to
one workspace goes in that catalog's `tvf_prefixes` instead, so only sessions
scoped to that catalog can read it with the engine's credentials (see
[read_parquet](../features/read-parquet.md#which-paths-the-engines-credentials-may-read)).

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
otlp_endpoint = "https://otel-collector:4317"   # https: audit events carry user identities and SQL
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

See [`docs/site/book/src/operations/openlineage.md`](./openlineage.md) for sink options and Marquez setup.

### Catalog

- Polaris or Nessie over Iceberg REST.
- SigV4 in the default build covers AWS Iceberg REST (Glue/S3 Tables federated endpoints) without the full Glue SDK image.
- Defer `full-backends` until runtime config actually dispatches to HMS or JDBC SQL catalogs. Glue Hive roots need no extra feature.

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

#### Worker scan budget

A worker splits `worker.memory_limit` into sub-budgets under `[worker.memory]`. The one that decides how many concurrent scans a worker can hold in RAM is `scan_budget`, a fifth of the limit by default. It is one budget per worker, shared by every scan the coordinator sends, so a worker with forty inbound scans admits decoded batches from all forty against the same capacity and parks the rest until the coordinator drains. A quarter of it is the fetch lane that charges each open parquet file while it streams; the rest admits decoded batches. When the coordinator's `sqe_worker_reported_scan_budget_free_bytes` sits at zero while the coordinator is busy elsewhere, the worker is waiting on the coordinator, not the other way round. Raise `scan_budget` only together with `memory_limit`: the sub-budgets must fit inside it.

```toml
[worker.memory]
scan_budget = "6GB"     # default: memory_limit / 5
flight_budget = "3GB"   # encoded frames awaiting shipment, default: memory_limit / 10
```

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
3. `parallel_probe_scan` and `parallel_scan` are already ON by default. Turn `parallel_probe_scan` OFF for workloads of small window and rollup queries (measured at SF1: TPC-DS q67 +45%, q30 +79%, against a +4.0% suite total inside noise). IcebergScanExec still advertises one output partition at the table provider; the parallelism comes from a post-planning rule instead, and flipping that wiring still needs a TPC-DS rerun (issue #414).
4. jemalloc A/B for glibc RSS parking after large sorts
5. Do **not** move coordinator to `memory_pool = "fair"`

Coordinator-to-worker HTTP/2 windows are tuned by default as of 2026-08-18 and
worth checking if you pinned an older config. Before that fix, the coordinator's
worker channels ran on h2's 65,535-byte stream and connection windows, which cap
one stream at `window / RTT` no matter how much bandwidth the link has: about
64 MB/s across a 1 ms hop, 6 MB/s across 10 ms. The connection window is shared,
so every scan fragment multiplexed to the same worker drew from the same 64 KB.
The defaults are now 8 MB per stream and 16 MB per connection, matching what the
coordinator's own Flight SQL server already used. Loopback rigs never showed the
old ceiling; a separate-host deployment does.

```toml
[coordinator.transport]
initial_stream_window_size = 8388608       # 8 MB
initial_connection_window_size = 16777216  # 16 MB
max_decoding_message_bytes = 16777216      # 16 MB, both ends of the worker link
```

`max_decoding_message_bytes` replaces tonic's 4 MiB receive default on the
worker's Flight server and on the coordinator's clients. Workers read it from
the same `[coordinator.transport]` section, so one value covers both ends. A
scan ticket that would still exceed it sheds its pushed runtime-filter
predicate and ships the fragment unfiltered rather than failing; watch the
`ticket_predicate_dropped_oversize` metric if scans get slower after raising
`runtime_filter_inlist_max_values`.

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
otlp_endpoint = "https://otel-collector.observability:4317"
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

Environment overrides follow the usual `SQE_*` / `SQE_METRICS__*` conventions (see `sqe.toml.example` and [`docs/site/book/src/deployment/configuration.md`](../deployment/configuration.md)).

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

## What production mode enforces

`coordinator.production_mode = true` (or `SQE_PRODUCTION_MODE=1`) turns the following into startup errors. The table is the one in `crates/sqe-core/src/config/production.rs`; a unit test fails when the two drift. Each `security.allow_*` waiver is an explicit acknowledgement, logged at startup.

| Rule | What it refuses |
|---|---|
| `no-dev-auth-providers` | `auth.providers` containing `anonymous` or `bearer_passthrough` (dev-only; accept any connection or skip local JWT validation). |
| `rate-limit-enabled` | `rate_limit.enabled = false` (allows query and auth flooding). |
| `no-insecure-transport` | `security.allow_insecure_transport = true` (cleartext distributed hops expose bearer tokens, worker secrets and vended S3 credentials). |
| `no-shared-service-identity` | Config-held `client_credentials` in `[auth].providers` or `[catalog(s).auth]` without `security.allow_shared_service_identity = true` (one identity for every user at Polaris/S3). |
| `worker-secret-required` | `coordinator.worker_urls` set with an empty `coordinator.worker_secret` and `allow_unauthenticated_workers = true` (any TCP-reachable client may register as a worker). |
| `max-sql-bytes` | `query.max_sql_bytes = 0` (disables the pre-parse statement size cap). |
| `explicit-distributed-credentials` | `coordinator.distributed_credentials = auto` with workers configured (ships the shared `[storage]` keys to workers for tables without vended credentials). |
| `vended-credentials-required` | A REST catalog without `require_vended_credentials = true` (FileIO would fall back to the shared `[storage]` S3 keys). |
| `policy-engine-configured` | `policy.engine = passthrough` without `security.allow_no_policy_engine = true` (no row filters, masks or column restrictions). |
| `grant-backend-configured` | `access_control.backend = none` without `security.allow_no_grant_backend = true` (GRANT and REVOKE would be rejected). |
| `ranger-mask-key` | `policy.engine = ranger` with an empty `policy.mask_key` and no `security.allow_unkeyed_hash_masks = true` (MASK_HASH would fall back to unsalted SHA-256). |
| `ranger-project-tags` | `policy.engine = ranger` with `policy.ranger.project-tags = false` and no `security.allow_unprojected_tags = true` (Spark would return columns SQE masks). |
| `no-hmac-tickets` | `worker.accept_hmac_tickets = true` (the HMAC key is the shared worker secret, so it proves nothing about who authored a scan ticket). |
| `verified-worker-advertise` | `coordinator.allow_unverified_advertise = true` (a secret holder could register an attacker URL and receive every user's scan tickets). |
| `no-inline-tvf-credentials` | `storage.tvf.allow_inline_credentials = true` (the default; caller-supplied object-store keys would bypass `allowed_object_store_prefixes`). |
| `mcp-https-resource-uri` | An `http://` `mcp.resource_uri` while MCP is enabled. |
| `mcp-https-authorization-servers` | An `http://` issuer in `mcp.authorization_servers` while MCP is enabled. |
| `https-telemetry-endpoints` | An `http://` OTLP, audit-export or (enabled) OpenLineage endpoint on anything but loopback. Audit events and lineage carry user identities and SQL text; a sidecar collector on `localhost` may stay plain. |
| `ranger-grant-backend-url` | `access_control.backend = ranger` with an empty `access_control.url` (the grant backend would silently be absent and GRANT / REVOKE NotImplemented). |
| `mcp-no-anonymous-auth` | `mcp.auth.mode = anonymous` while MCP is enabled. |

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

- [Deployment configuration](../deployment/configuration.md)
- [OpenLineage operations](./openlineage.md)
- Full audit: `docs/internal/audit/2026-07-10-sqe-full-audit.md` (repo only, not published)
- Example config: `sqe.toml.example` in the repo root
- `QUICKSTART.md` in the repo root for local validation before production cutover
## Recent Audit Remediation Notes (2026-07-11)

- `[rate_limit]` section and full production examples now documented in `sqe.toml.example` (see MRs !590, !591).
- Stale `AUDIT.md` and the full `2026-07-10-sqe-full-audit.md` refreshed with progress (MRs !592, !594).
- Jemalloc and memory recommendations added to example (MR !593).
- Babysat fixes for several R items verified and pushed (!579–!581).
- Perf Group 4 branches currently at main; MRs !585–!588 have notes.
- Production config validator, DML audit, observability wiring, and CI for write/distributed have seen substantial progress via prior and current MRs.

Refer to the full audit for the prioritized plan. Keep `sqe.toml.example` and this guide in sync as more items land.
