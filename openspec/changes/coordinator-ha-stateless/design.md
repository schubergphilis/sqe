## Context

Today a Flight SQL client authenticates once, the coordinator mints a UUID, stores `Arc<Session>` in a `DashMap` keyed by that UUID (`crates/sqe-coordinator/src/session_manager.rs:39-43`), and returns the UUID as a bearer token. Every later RPC carries that UUID. The replica that minted it is the only one that can resolve it.

The recommendation: validate the OIDC JWT directly instead of trusting a server-minted id. The JWT already carries everything a replica needs (issuer, subject, expiry, roles), and `sqe-auth` already validates JWTs against the IdP JWKS during the password-grant flow. Moving that validation onto the per-request hot path, with a short-TTL verification cache, makes any replica able to serve any client with zero shared state. A shared session store (Redis/Postgres) was the alternative; it is rejected for Phase 1 because it adds an external dependency and a new failure mode to solve a problem the JWT already solves.

## Goals / Non-Goals

**Goals:**
- Any coordinator replica can validate and serve any client request without shared session state (Phase 1).
- N replicas behind a Service survive a single-replica failure with bounded, documented client impact.
- Global rate limits and worker visibility across replicas (Phase 2).

**Non-Goals:**
- Transparent failover of in-flight queries. A query running on a failed replica is lost; the client retries. Mid-query state migration is out of scope.
- Sharing in-flight result streams across replicas. Result data stays on the owning replica until drained.
- Multi-region active-active. This change targets multiple replicas in one cluster.

## Architecture

### Phase 1: stateless session validation

```
                 ┌──────────────────────────────────────────┐
   client  ------->  Service (round-robin / least-conn)       │
   (JWT bearer)  └──────┬───────────────┬───────────────┬────┘
                        │               │               │
                   ┌────v────┐     ┌────v────┐     ┌────v────┐
                   │ coord-0 │     │ coord-1 │     │ coord-2 │
                   │ JWT     │     │ JWT     │     │ JWT     │
                   │ verify  │     │ verify  │     │ verify  │
                   │ cache   │     │ cache   │     │ cache   │
                   └────┬────┘     └────┬────┘     └────┬────┘
                        └───────────────┼───────────────┘
                                        │ JWKS fetch (cached)
                                  ┌─────v─────┐
                                  │   OIDC    │
                                  │   IdP     │
                                  └───────────┘
```

Every request carries the IdP-issued JWT as the bearer token. The replica:
1. Checks a per-replica verification cache keyed by `sha256(jwt)`.
2. On miss, validates signature against the cached JWKS, checks `exp`/`nbf`/`iss`/`aud`, extracts subject and roles, and caches the parsed `Identity` until the JWT's own `exp` (or a configured max, whichever is sooner).
3. Builds or reuses a per-replica `Session` (catalog token, DataFusion `SessionContext`) for that identity.

The `DashMap<String, Arc<Session>>` stays, but its key becomes `sha256(jwt)` and it is a cache, not the authority. A cold replica rebuilds it on first use.

### The lifecycle is not fully stateless

Stateless *validation* is not a stateless *query lifecycle*. This is the part that is easy to overclaim.

A Flight `DoGet` ticket points at an in-flight result stream that lives in the memory of the replica that planned the query. The query tracker (`crates/sqe-coordinator/src/query_tracker.rs:94-97`) and the result cache are per-replica. So:

- The Flight ticket gains an `owning_replica` field (the pod's stable DNS name from the headless Service). A `DoGet` that lands on the wrong replica either proxies to the owner or returns a redirect the client follows.
- A query whose owning replica dies is gone. `GetFlightInfo` / `DoGet` returns a retryable error; the client re-submits. Documented, not hidden.

```
  GetFlightInfo (replica picks owner = self)
        │
        v  ticket { query_id, owning_replica: "coord-1.sqe-headless" }
  DoGet --> Service --> coord-2  -- owner != self --> proxy/redirect --> coord-1 --> result stream
```

### Rate limits and per-user concurrency under N replicas

`QueryRateLimiter` holds per-user and global token buckets in process memory (`crates/sqe-coordinator/src/rate_limiter.rs:131-135`). With N replicas and round-robin balancing, a per-user limit of R becomes an effective R*N. Phase 1 accepts this drift and documents it: set per-replica limits to `ceil(target / replicas)` as a stopgap. Phase 2 moves the buckets to a shared store for exact global enforcement.

Per-user concurrency semaphores have the same N-fold drift and the same Phase-1 / Phase-2 treatment.

### Worker registry across replicas

`WorkerRegistry` already supports heartbeat-based discovery: `register_heartbeat(url)` adds a worker on first contact (`crates/sqe-coordinator/src/worker_registry.rs:224-238`). That is the cheapest path to multi-replica visibility: point every worker at the Service, and the heartbeat reaches whichever replica answers, so over a few heartbeat intervals each replica converges on the full set. Phase 2 hardens this with one of:

| Option | Behaviour | Trade-off |
|---|---|---|
| Heartbeat-to-all (recommended Phase 2 start) | Worker resolves the headless Service and heartbeats every replica | No external store; convergence is eventual, bounded by heartbeat interval |
| Shared store (Redis/Postgres) | Workers register once; replicas read the shared set | Exact, instant; adds a dependency and a failure mode |
| Gossip | Replicas exchange registry deltas | No external store; most code to write and test |

Recommendation: heartbeat-to-all for Phase 2, shared store only if a deployment already runs Redis/Postgres for the rate limiter.

## Key Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Session authority | Validate the OIDC JWT directly | Zero shared state; reuses existing JWKS validation; the JWT already carries identity + expiry |
| Shared session store | Rejected for Phase 1 | Adds a dependency and failure mode to solve a problem the JWT already solves |
| In-flight query on failover | Lost; client retries | Mid-query migration is large, fragile, and rarely worth it for analytics workloads |
| Result routing | Ticket encodes owning replica | Result streams are replica-local; the ticket must say where |
| Rate limits Phase 1 | Per-replica, divided by N | Accepts bounded drift; avoids blocking N replicas on a shared store |
| Worker registry Phase 2 | Heartbeat-to-all | Registry already does heartbeat discovery; no new dependency |

## Risks

| Risk | Mitigation |
|---|---|
| JWT validation on every RPC adds latency | Short-TTL verification cache keyed by `sha256(jwt)`; JWKS cached with rotation refresh |
| Long-lived JWTs cannot be revoked before `exp` | Honour the IdP's introspection endpoint or a deny-list for forced revocation; document the trade-off |
| Client cannot reach the owning replica through the LB | Headless Service for pod-addressable DNS; proxy fallback when redirect is not followed |
| Rate-limit drift confuses operators | Emit `sqe_ratelimit_replica_count` and document the divide-by-N rule until Phase 2 |
| Plaintext token file persistence misused as HA | Deprecate file persistence for HA; JWT validation removes the need to persist sessions at all |
