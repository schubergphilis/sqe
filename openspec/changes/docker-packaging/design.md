## Context

SQE is a Rust-based distributed SQL engine with a coordinator/worker architecture. Both roles share the majority of their dependencies (DataFusion, iceberg-rust, Arrow Flight, Keycloak auth). The project targets Kubernetes deployment. Currently there is no packaging or container strategy defined.

Operators need an interactive SQL CLI for debugging and ad-hoc queries against running clusters, and this CLI must always be version-matched with the server it connects to.

## Goals / Non-Goals

**Goals:**
- Single Docker image containing both `sqe-server` and `sqe-cli` binaries
- `sqe-server` operates as coordinator or worker based on runtime configuration
- `sqe-cli` available via `docker exec` / `kubectl exec` for interactive SQL
- Minimal image size via multi-stage build
- Same image used across all environments (dev, staging, prod)

**Non-Goals:**
- Separate images per role (coordinator vs worker)
- Embedding a web UI in the container
- Auto-discovery or auto-clustering (handled by K8s service discovery)
- Windows container support

## Decisions

### 1. Single binary with mode flag over separate coordinator/worker binaries

**Decision**: `sqe-server` is one binary; `SQE_MODE=coordinator|worker` selects behavior at startup.

**Rationale**: Coordinator and worker share ~80% of code (DataFusion, catalog, auth, Flight). Two binaries would duplicate most dependencies and complicate version management. A single binary with mode selection is the pattern used by CockroachDB, Consul, and etcd.

**Alternative considered**: Separate `sqe-coordinator` and `sqe-worker` binaries. Rejected because it doubles the binary count without meaningful benefit — both link the same libraries.

### 2. Environment variable for mode selection over config file

**Decision**: Primary mode selection via `SQE_MODE` env var, with config file as optional override.

**Rationale**: Env vars are the native configuration mechanism in Kubernetes (via Deployment spec). Config files require ConfigMap mounts. Env var is simpler for the common case; config file supports advanced tuning (thread counts, memory limits, listen addresses).

**Alternative considered**: Config file only. Rejected because it adds unnecessary K8s ConfigMap boilerplate for the most basic setting.

### 3. Multi-stage Dockerfile with Debian slim base

**Decision**: Build stage uses `rust:bookworm`, runtime stage uses `debian:bookworm-slim`.

**Rationale**: `bookworm-slim` provides glibc and OpenSSL (needed for TLS to Polaris/Keycloak) at ~80MB base. Alpine/musl would require static linking or musl builds of all native dependencies (OpenSSL, ring), which complicates the build for marginal size savings.

**Alternative considered**: `distroless/cc` — smaller but harder to debug (no shell). Since we need `sqe-cli` to be usable via `exec`, having a shell available is valuable. Also considered Alpine; rejected due to musl compatibility friction with Rust crypto crates.

### 4. sqe-cli as a separate binary, not a subcommand of sqe-server

**Decision**: `sqe-cli` is its own binary target, not `sqe-server cli`.

**Rationale**: Keeps concerns separate — the server binary handles serving, the CLI handles client interaction. The CLI can be built and tested independently. `docker exec -it <ctr> sqe-cli` is cleaner than `docker exec -it <ctr> sqe-server cli`.

### 5. Cargo workspace binary layout

**Decision**: `sqe-server` binary lives in the `sqe-coordinator` crate (which depends on `sqe-worker`). `sqe-cli` lives in a new `sqe-cli` crate.

**Rationale**: The coordinator crate already orchestrates workers, so it's the natural home for the unified server entry point. The CLI is a pure client with no server dependencies — it only needs `arrow-flight`, `tonic`, `clap`, and a line editor.

## Risks / Trade-offs

- **Larger image than needed for a single role** → Both coordinator and worker code are in the image even though only one runs. Mitigation: the overhead is just binary size (~10-20MB extra), not runtime memory. Acceptable trade-off for operational simplicity.

- **Mode misconfiguration** → Deploying without `SQE_MODE` set could start in wrong mode. Mitigation: `sqe-server` SHALL refuse to start if `SQE_MODE` is not set (fail-closed, no default).

- **CLI version drift in dev** → Developer running local CLI against remote cluster with different version. Mitigation: CLI prints server version on connect; version mismatch produces a warning. In-container CLI is always version-matched.
