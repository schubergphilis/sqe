# SQE On-Call Runbook

This is the 3 AM runbook. You were paged, you are half awake, and something is
broken. Each section follows the same shape: symptom, likely cause, diagnosis,
resolution, escalation. Commands assume `kubectl` against the namespace where
SQE runs. Set it once:

```bash
export NS=sqe        # change to your namespace
kubectl config set-context --current --namespace="$NS"
```

Metric names below are the real ones the engine exports (see
`crates/sqe-metrics/src/lib.rs`). Scrape the coordinator or worker metrics port
(default `:9090`) or query them through Prometheus.

A fast triage loop before you dig in:

```bash
kubectl get pods -l app.kubernetes.io/name=sqe -o wide
kubectl get events --sort-by=.lastTimestamp | tail -20
```

---

## 1. Worker crashloop

**Symptom.** One or more worker pods in `CrashLoopBackOff` or `Error`. Queries
that need distribution slow down or fail. `sqe_healthy_workers` drops below
`worker.replicas`.

**Likely causes.**

- Worker secret mismatch (the coordinator and worker secrets differ, or one is
  missing). The engine refuses to boot.
- `/readyz` never goes green: the worker cannot reach the coordinator or the
  catalog.
- Memory limit vs pod limit mismatch: the kernel OOMKills the pod before
  DataFusion spills.
- Spill directory not writable (read-only root filesystem, no `/tmp` emptyDir,
  or `EROFS`).

**Diagnosis.**

```bash
# Which workers, and why they restarted.
kubectl get pods -l app.kubernetes.io/component=worker
kubectl describe pod <worker-pod> | sed -n '/Last State/,/Ready/p'

# The actual boot error. A secret mismatch prints a validation message naming
# worker.worker_secret. An OOMKill shows "OOMKilled" as the last-state reason.
kubectl logs <worker-pod> --previous | tail -50
```

Confirm the secret matches on both sides:

```bash
# Both must resolve to the same value.
kubectl get deploy/<release>-coordinator -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="SQE_COORDINATOR__WORKER_SECRET")]}'
kubectl get deploy/<release>-worker      -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="SQE_WORKER__WORKER_SECRET")]}'
```

Memory pressure and spill behaviour:

```promql
# Worker engine limit (bytes). Compare against the pod memory limit.
sqe_coordinator_memory_limit_bytes
# Spill activity. Climbing counters mean the engine is defending itself.
rate(sqe_sort_spill_bytes_total[5m])
rate(sqe_join_spill_bytes_total[5m])
```

```bash
# Pod memory limit, for the comparison above.
kubectl get pod <worker-pod> -o jsonpath='{.spec.containers[0].resources.limits.memory}'
```

If the last-state reason is `OOMKilled`, the engine `memory_limit` is too close
to the pod limit. The chart targets ~75% of the pod limit on purpose (see
`config.worker.memory_limit` in `values.yaml`).

If logs show a spill write error (`EROFS`, `Permission denied`,
`No such file or directory` under the spill dir), the writable `/tmp` emptyDir
is missing or the spill dir points outside it.

**Resolution.**

- Secret mismatch: set `workerSecret` once and let the chart inject the same
  value into both tiers. `helm upgrade <release> deploy/helm/sqe --reuse-values
  --set workerSecret.value=<shared>` (or point both at the same
  `workerSecret.existingSecret`). See `docs/deployment.md` ISSUE-218.
- OOMKilled: lower `config.worker.memory_limit` to ~75% of the pod limit, or
  raise the pod limit. Roll the workers.
- Spill on read-only root: confirm the `/tmp` emptyDir mount exists and
  `config.worker.spill_dir` lives under `/tmp`. The chart wires this by
  default; a custom overlay may have dropped it.

**Escalation.** If logs show a panic or a repeated DataFusion internal error
(not a config or OOM cause), capture `kubectl logs --previous`, the pod spec,
and the failing query, then page the engine on-call. Tag ISSUE-220 if the
worker registers and then disappears (registry flapping, see section 5).

---

## 2. Polaris (catalog) down or unauthenticated

**Symptom.** Queries that touch new tables fail. `SHOW TABLES`, `DESCRIBE`, and
first-touch reads error. Already-planned, already-cached queries may still run
until the metadata cache TTL expires.

**Likely cause.** The Iceberg REST catalog (Polaris) is unreachable, returning
5xx, or rejecting the bearer token (401/403). SQE wraps catalog calls in a
circuit breaker; repeated failures trip it open.

**Diagnosis.**

```promql
# 0 = closed (healthy), 1 = half_open (probing), 2 = open (failing fast).
sqe_catalog_circuit_breaker_state
# Catalog latency. A spike before the breaker opens is the leading signal.
histogram_quantile(0.95, rate(sqe_catalog_request_duration_seconds_bucket[5m]))
```

```bash
# Coordinator logs around catalog calls.
kubectl logs deploy/<release>-coordinator | grep -iE "catalog|circuit|polaris|401|403|5[0-9][0-9]" | tail -40

# Reach the catalog from inside the cluster (uses the configured URL).
kubectl exec deploy/<release>-coordinator -- \
  curl -sS -o /dev/null -w '%{http_code}\n' http://polaris:8181/api/catalog/v1/config
```

Expected error codes: a `401`/`403` is auth (token or Polaris RBAC), a
`5xx`/connection refused is availability. With the breaker `open` (state 2), SQE
fails catalog calls fast instead of hanging.

**What still works.** Queries against tables whose metadata is already cached
keep running until `catalog.metadata_cache_ttl_secs` expires (default 30s). No
new table resolution, no writes, no DDL.

**Resolution.**

- Availability: restore Polaris. The breaker moves `open -> half_open -> closed`
  on its own once probes succeed. No SQE restart needed.
- Auth: a `401`/`403` for everyone points at Polaris or the OIDC provider (see
  section 3). A `403` for one user is that user's Polaris privileges, not an
  outage.

**Escalation.** If the breaker stays `open` after Polaris returns healthy from
its own health check, capture the coordinator logs and the breaker metric and
page the catalog owner. If auth fails cluster-wide, jump to section 3.

---

## 3. OIDC provider down

**Symptom.** New logins fail. The Flight SQL handshake errors for clients that
present a username/password. Token refresh fails for long-lived sessions.

**Likely cause.** The OIDC provider (Keycloak, Auth0, Okta) is unreachable or
returning errors. SQE has no service account: every session authenticates
against the provider, so a provider outage blocks new authentication.

**Diagnosis.**

```promql
# Failed auth attempts by provider.
sum by (provider) (rate(sqe_auth_attempts_total{status="failed"}[5m]))
# Failed token refreshes. Climbing here means active sessions are about to drop.
rate(sqe_token_refresh_total{status="failed"}[5m])
# Handshake latency. A jump precedes failures when the provider is slow not down.
histogram_quantile(0.95, rate(sqe_auth_duration_seconds_bucket[5m]))
```

```bash
kubectl logs deploy/<release>-coordinator | grep -iE "auth|oidc|token|jwks|handshake" | tail -40

# Reach the provider token endpoint from the coordinator.
kubectl exec deploy/<release>-coordinator -- \
  curl -sS -o /dev/null -w '%{http_code}\n' \
  https://keycloak.example.com/realms/iceberg/protocol/openid-connect/token
```

**What fails vs what keeps working.** New logins and token refreshes fail.
Sessions with a still-valid access token keep running queries until the token
expires or the session hits its idle/absolute timeout. The blast radius grows
as tokens age out.

**Resolution.**

- Restore the provider. Authentication recovers without an SQE restart.
- If only refresh fails while the provider is up, check
  `auth.token_refresh_buffer_secs` and clock skew between SQE and the provider.
- A cluster-wide `401` from the catalog (section 2) with healthy Polaris often
  traces back here: the provider is issuing tokens Polaris rejects.

**Escalation.** Provider outages are usually owned by the identity team. Hand
them the failed-attempt rate by provider and the token-endpoint HTTP code from
the curl above.

---

## 4. Coordinator OOM / memory pressure

**Symptom.** Coordinator pod restarts with `OOMKilled`, or queries slow down
and start spilling heavily. Client sessions drop on restart (state is
process-local; see section on HA in `docs/deployment.md`).

**Likely cause.** A large result set, a heavy single-node plan, or many
concurrent queries pushed the coordinator past its engine memory limit. Spill
is the defense; an OOMKill means spill could not keep up or could not run.

**Diagnosis.**

```promql
# 0.0 - 1.0. Sustained > ~0.85 means the engine is near its limit.
sqe_coordinator_memory_pressure
# Used vs limit (bytes).
sqe_coordinator_memory_used_bytes
sqe_coordinator_memory_limit_bytes
# Spill counters. Rising = the engine is shedding memory to disk as intended.
rate(sqe_sort_spill_bytes_total[5m])
rate(sqe_join_spill_bytes_total[5m])
# Concurrency. A spike in active sessions often precedes pressure.
sqe_active_sessions
```

```bash
kubectl describe pod <coordinator-pod> | grep -i -A3 "Last State"
kubectl get pod <coordinator-pod> -o jsonpath='{.spec.containers[0].resources.limits.memory}'
```

If the last state is `OOMKilled` and spill counters were flat, spill was
disabled or the spill dir was not writable. If spill counters were climbing and
it still OOMKilled, the engine `memory_limit` sits too close to the pod limit.

**Resolution.**

- Confirm `config.coordinator.spill_to_disk = true` and the `/tmp` emptyDir is
  mounted (the chart does both by default).
- Lower `config.coordinator.memory_limit` to ~70-75% of the pod limit, or raise
  the pod limit, then roll the coordinator.
- Cap per-query and concurrency budgets in `[query]`: `max_query_memory`,
  `max_concurrent_queries`, `max_result_rows`. Per-user limits live under
  `[rate_limit]`.

**Escalation.** Repeated OOMKills after lowering the engine limit, with spill
active, means a single query is too large for the node. Capture the query from
the audit log and the memory metrics, then page the engine on-call.

---

## 5. Distributed query hangs / worker registry flapping

**Symptom.** Distributed queries stall and never return, or `sqe_healthy_workers`
oscillates as workers register and drop. Workers look `Running` and `Ready` in
`kubectl` but the coordinator does not keep them.

**Likely cause.** Worker advertise-URL or reachability mismatch (ISSUE-220): the
worker registers an address the coordinator cannot connect back to, or the
heartbeat path is blocked. A worker secret mismatch can also cause register
-then-reject churn.

**Diagnosis.**

```promql
# Should equal worker.replicas and hold steady. Oscillation = flapping.
sqe_healthy_workers
# Fragment throughput per worker. A worker stuck at 0 while others move is suspect.
sum by (pod) (rate(sqe_worker_fragments_executed_total[5m]))
```

```bash
# Heartbeat and registration churn on the coordinator.
kubectl logs deploy/<release>-coordinator | grep -iE "register|heartbeat|worker|advertise|unreachable" | tail -50

# Can the coordinator reach a worker on the Flight port?
kubectl exec deploy/<release>-coordinator -- \
  curl -sS -o /dev/null -w '%{http_code}\n' http://<release>-worker-0.<release>-worker-headless:50052
```

**Resolution.**

- Confirm workers register the headless-service DNS name the coordinator can
  resolve, not a pod-local or loopback address. See ISSUE-220 for the
  advertise-url fix.
- Rule out a secret mismatch (section 1): a worker that registers then gets
  rejected churns the registry.
- A genuinely hung query (workers healthy, fragments at 0) hits the
  `query.timeout_secs` ceiling and is cancelled; lower it if hangs are common
  while you chase the root cause.

**Escalation.** Flapping that survives a confirmed-correct advertise URL and a
matching secret is an engine bug. Capture coordinator logs, `sqe_healthy_workers`
over time, and the worker logs, then page the engine on-call and reference
ISSUE-220.

---

## Escalation summary

| Surface | First responder | Page next |
|---|---|---|
| Worker crashloop, coordinator OOM, registry flap | SQE on-call | Engine on-call |
| Catalog (Polaris) outage | SQE on-call | Catalog owner |
| OIDC provider outage | SQE on-call | Identity team |

Always attach: the failing pod's `kubectl logs --previous`, the relevant metric
over the incident window, and the offending query from the audit log
(`metrics.audit_log_path`) when one exists.
