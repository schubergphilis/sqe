## Context

Replacing the Chameleon Trino fork (DCAF branch) with a purpose-built Rust SQL engine. The trino-fork implements Keycloak password auth → token passthrough to Polaris/S3 via custom SPI (`CredentialCarryingPrincipal`) and Iceberg REST catalog session injection. SQE rebuilds this as first-class architecture rather than patches.

Full design spec: `docs/superpowers/specs/2026-03-14-sqe-core-engine-design.md`

## Goals / Non-Goals

**Goals:**
- Per-user Keycloak token passthrough to Polaris and S3 (credential vending)
- Distributed query execution scaling from single-node to petabyte
- Full SQL write path (CTAS, INSERT, MERGE, DELETE, DROP, ALTER, views)
- Flight SQL + Trino wire compat
- Drop-in replacement for the trino-fork in the quickstart stack

**Non-Goals:**
- OPA/Cedar policy enforcement (stub only)
- dbt adapter (separate project)
- Coordinator HA / leader election
- Helm charts / production deployment

## Architecture

```
Client (Flight SQL / Trino HTTP)
  → Coordinator: parse → auth → plan → optimize → distribute
    → Workers (0..N): execute fragments with user credentials → stream results
      → Polaris REST (bearer token) → S3 (vended credentials)
```

10 crates in a Cargo workspace. Two binaries: `sqe-coordinator`, `sqe-worker`.

Key patterns:
- **Auth**: Keycloak ROPC → token cache + background refresh → Session struct carries token
- **Catalog**: Per-session iceberg-rust REST catalog with bearer token, S3 cred vending
- **Distribution**: Coordinator splits PhysicalPlan by Iceberg manifests, sends to workers via Arrow Flight with credentials in metadata. Workers are read-only executors. Writes happen on coordinator.
- **Trino compat**: Thin axum HTTP adapter translating v1/statement → internal execution

## Key Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Distribution | Custom over Arrow Flight | Auth passthrough is core constraint, not a Ballista bolt-on |
| Write execution | Coordinator-only | Avoids write idempotency / orphan file issues |
| SQL parser | Wrap sqlparser-rs | No fork, post-transform for custom statements |
| Write strategy | Merge-on-Read (position deletes) | Simpler write path, compaction deferred |
| Token storage | Tokens only, no passwords | Session re-auth on full token expiry |
| S3 access | Credential vending from Polaris | No static S3 keys in production |

## Risks

| Risk | Mitigation |
|---|---|
| `datafusion-proto` codec for iceberg-rust plan nodes | Custom codec extensions, same approach as Ballista |
| iceberg-rust write path maturity | Validate early with CTAS integration test |
| Trino wire compat surface area | Scope to v1/statement + basic auth only |
| Long query token/credential expiry | Coordinator refreshes and pushes to workers |
