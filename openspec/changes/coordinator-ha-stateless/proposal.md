## Why

The coordinator is a single-replica single point of failure. The Helm chart ships `coordinator.replicas: 1` (`deploy/helm/sqe/values.yaml:17`) and that is not an accident: almost every piece of coordinator state lives in process-local memory.

- Sessions are a `DashMap<String, Arc<Session>>` keyed by a server-minted UUID (`crates/sqe-coordinator/src/session_manager.rs:39-43`). The UUID is handed back to the client as a bearer token. A second replica has never seen that UUID, so it rejects the client.
- The worker registry, query tracker / history, rate-limiter token buckets, and per-user concurrency limits are all per-process (`crates/sqe-coordinator/src/worker_registry.rs:87`, `crates/sqe-coordinator/src/query_tracker.rs:94-97`, `crates/sqe-coordinator/src/rate_limiter.rs:22-135`).
- Session persistence exists only as an opt-in best-effort file snapshot, off by default, and it writes access tokens to disk in plaintext (`crates/sqe-coordinator/src/bin/sqe_server.rs:763-806`).

A coordinator restart drops every in-flight query and invalidates every client's session id. Running a second replica today is unsafe: a client load-balanced to the wrong replica gets `session not found`, and rate limits silently double.

This change makes the coordinator horizontally scalable so a restart or a node failure does not take the cluster down.

## What Changes

The work is phased. Phase 1 removes the hard blocker (server-minted session ids) so any replica can serve any client. Later phases centralize the remaining shared state.

**Phase 1 -- stateless session validation + N replicas:**
1. Replace server-minted session-id bearer tokens with direct validation of the user's OIDC JWT on every request. The JWT is self-describing (issuer, expiry, subject, roles); any replica can validate it against the IdP's JWKS without shared state.
2. Keep a per-replica session cache as an optimization (catalog token, parsed identity), keyed by a stable hash of the JWT, populated lazily on cache miss. The cache is no longer the source of truth.
3. Run N coordinator replicas behind a Kubernetes `Service` with a `PodDisruptionBudget`. Document that in-flight queries are lost on failover and the client retries.
4. Encode the owning replica into the Flight ticket so a `DoGet` for an in-flight result routes back to the replica that holds the stream (headless Service + pod addressing).

**Phase 2 -- shared worker registry + global limits:**
5. Move the worker registry behind a shared store (Redis or Postgres) so all replicas see the same healthy-worker set, or have every worker heartbeat to all replicas.
6. Move rate-limiter buckets and per-user concurrency semaphores to a shared store so limits are global, not per-replica.

**Phase 3 -- centralized query history:**
7. Optionally centralize the query tracker / history so `SHOW QUERIES` and the web UI see all replicas. In-flight result streams remain replica-local; only metadata is shared.

## Capabilities

### New Capabilities
- `coordinator-ha-stateless-session`: validate the OIDC JWT directly; no server-minted session id required for cross-replica routing.
- `coordinator-ha-replicas`: run N coordinator replicas behind a Service + PDB with documented failover semantics.
- `coordinator-ha-shared-registry`: shared or fan-out worker registry so every replica sees the same workers (Phase 2).
- `coordinator-ha-global-limits`: global rate limits and per-user concurrency across replicas (Phase 2).

### Modified Capabilities
- `session-management`: session validation becomes stateless; the in-memory `DashMap` becomes a cache, not the authority.

## Impact

- `sqe-coordinator`: `SessionManager` gains a JWT-validation path; the Flight ticket format gains an owning-replica field; `WorkerRegistry`, `QueryRateLimiter`, and per-user semaphores gain a shared-store backend (Phase 2).
- `sqe-auth`: JWT validation against JWKS already exists for the OIDC flow; this reuses it on the hot path with a short-TTL verification cache.
- `deploy/helm/sqe`: `coordinator.replicas` default raised once Phase 1 lands; add a `PodDisruptionBudget`, a headless `Service` for pod addressing, and an optional Redis/Postgres dependency for Phase 2.
- No change to the worker protocol. Workers already authenticate to Polaris and S3 as the user via token passthrough.

## Success Criteria

- With `session.validation = "jwt"`, any replica validates any client's JWT; no client sees `session not found` when load-balanced across replicas.
- A rolling restart of one replica drops only that replica's in-flight queries; the others keep serving (PDB `minAvailable = replicas - 1`).
- A `DoGet` that lands on a non-owning replica routes to the owner; on failover the call returns a retryable error and a client re-submit succeeds.
- Phase 2: a per-user rate limit holds to `target` globally across replicas via the shared store; Phase 1 documents the divide-by-N drift.

## Migration

Two moving parts. Server side: flip `session.validation` from `session_id` to `jwt` once Phase 1 is verified. Client side: when the server runs in `jwt` mode, clients send the raw IdP-issued JWT as the bearer token instead of the server-minted session id. The two modes can run in parallel during a rollout by keeping `session_id` accepted alongside `jwt` for one release, then dropping `session_id`.

## Rollback

Phase 1 is config-gated. `session.validation = "session_id"` keeps the current server-minted-UUID behaviour; `session.validation = "jwt"` enables stateless validation. Default stays `session_id` until Phase 1 is verified, then flips. Rolling back to one replica is a Helm value change with no data migration. Phase 2 shared stores are optional dependencies: if the store is unreachable, the coordinator falls back to per-replica limits and logs a warning rather than failing closed.
