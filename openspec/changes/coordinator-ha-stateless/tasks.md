## 1. Stateless session validation (Phase 1)

- [ ] 1.1 Add `session.validation` config (`session_id` | `jwt`), default `session_id` in `sqe-core`
- [ ] 1.2 Add a JWT verification path to `SessionManager`: validate against cached JWKS, check `exp`/`nbf`/`iss`/`aud`, extract `Identity` (reuse `sqe-auth` JWKS validation)
- [ ] 1.3 Re-key the session `DashMap` from server-minted UUID to `sha256(jwt)`; treat it as a cache (lazy rebuild on miss), not the authority
- [ ] 1.4 Add a short-TTL verification cache (moka) keyed by `sha256(jwt)`, TTL = min(jwt `exp`, configured max)
- [ ] 1.5 Update the Flight handshake to accept the IdP JWT directly as the bearer token when `validation = "jwt"`
- [ ] 1.6 Unit test: two independent `SessionManager` instances both validate the same JWT and resolve the same identity with no shared state
- [ ] 1.7 Unit test: expired / wrong-issuer / bad-signature JWTs are rejected with no info leak

## 2. Result routing across replicas (Phase 1)

- [ ] 2.1 Add `owning_replica` (pod DNS name) to the Flight ticket payload
- [ ] 2.2 On `DoGet`, if `owning_replica != self`, proxy to the owner (or return a retryable redirect)
- [ ] 2.3 Resolve the pod's stable name from the headless Service env / downward API
- [ ] 2.4 Integration test: ticket minted on replica A, `DoGet` lands on replica B, result is served correctly
- [ ] 2.5 Integration test: owning replica killed mid-query, `DoGet` returns a retryable error, client re-submits and succeeds

## 3. Helm: N replicas + Service + PDB (Phase 1)

- [ ] 3.1 Raise `coordinator.replicas` default once Section 1+2 verified; keep overridable
- [ ] 3.2 Add a headless `Service` for pod-addressable DNS
- [ ] 3.3 Add a `PodDisruptionBudget` (minAvailable = replicas - 1)
- [ ] 3.4 Document the divide-by-N rate-limit stopgap in `values.yaml` comments
- [ ] 3.5 Integration test: rolling restart of one replica does not drop traffic on the others

## 4. Global rate limits + per-user concurrency (Phase 2)

- [ ] 4.1 Define a `RateLimitStore` trait; implement `InMemory` (current) and a shared backend (Redis token bucket or Postgres advisory)
- [ ] 4.2 Move `QueryRateLimiter` per-user + global buckets behind the trait
- [ ] 4.3 Move per-user concurrency semaphores behind the same trait
- [ ] 4.4 Fall back to per-replica limits + warn when the shared store is unreachable (fail open, never fail closed on auth-valid traffic)
- [ ] 4.5 Unit test: two replicas sharing the store enforce one global limit
- [ ] 4.6 Metric: `sqe_ratelimit_backend` (in_memory | shared), `sqe_ratelimit_store_errors_total`

## 5. Shared / fan-out worker registry (Phase 2)

- [ ] 5.1 Workers resolve the headless Service and heartbeat every coordinator replica
- [ ] 5.2 Verify each replica converges on the full healthy-worker set within N heartbeat intervals
- [ ] 5.3 Optional shared-store registry behind a `WorkerRegistryStore` trait for deployments already running Redis/Postgres
- [ ] 5.4 Integration test: 3 coordinators + 4 workers, every coordinator distributes across all 4

## 6. Centralized query history (Phase 3, optional)

- [ ] 6.1 Define a `QueryHistoryStore` trait; default stays per-replica in-memory
- [ ] 6.2 Optional shared backend so `SHOW QUERIES` aggregates across replicas (metadata only; result streams stay replica-local)
- [ ] 6.3 Integration test: query started on replica A is visible in `SHOW QUERIES` issued to replica B
