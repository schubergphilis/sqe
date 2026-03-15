## 1. Cargo Workspace Setup

- [x] 1.1 Add `sqe-server` binary target to `sqe-coordinator` crate (`src/bin/sqe_server.rs`)
- [x] 1.2 Create `sqe-cli` crate in workspace with dependencies (`clap`, `arrow-flight`, `tonic`, `reedline`, `comfy-table`)
- [x] 1.3 Add shared version constant to `sqe-core` for use by both binaries

## 2. sqe-server Binary

- [x] 2.1 Implement CLI argument parsing with `clap` (`--config`, `--version`)
- [x] 2.2 Implement mode selection from `SQE_MODE` env var (coordinator/worker, case-insensitive, fail if missing/invalid)
- [x] 2.3 Implement TOML config file loading with `--config` flag (overrides env vars)
- [x] 2.4 Implement SIGTERM/SIGINT graceful shutdown with configurable timeout
- [x] 2.5 Implement `/healthz` liveness and `/readyz` readiness HTTP endpoints
- [x] 2.6 Wire up coordinator startup path (SQL parser, planner, scheduler, Flight SQL server)
- [x] 2.7 Wire up worker startup path (DataFusion executor, Flight data server, coordinator registration)

## 3. sqe-cli Binary

- [x] 3.1 Implement CLI argument parsing (`--host`, `--port`, `--token`, `--user`, `--format`, `-e`)
- [x] 3.2 Implement Flight SQL client connection with bearer token auth
- [x] 3.3 Implement Keycloak OIDC password grant flow for `--user` authentication
- [x] 3.4 Implement interactive REPL with `reedline` (history, multi-line SQL, Ctrl+C/Ctrl+D handling)
- [x] 3.5 Implement output formatting: ASCII table (default), CSV, JSON
- [x] 3.6 Implement version display on connect with server version mismatch warning

## 4. Dockerfile

- [x] 4.1 Create multi-stage Dockerfile: build stage (`rust:bookworm`), runtime stage (`debian:bookworm-slim`)
- [x] 4.2 Configure non-root user (`sqe`, UID 1000) in runtime stage
- [x] 4.3 Set entrypoint to `sqe-server`, empty CMD
- [x] 4.4 Add OCI labels (version, created, revision, description) via build args
- [x] 4.5 Add `.dockerignore` to exclude target/, docs/, .git/

## 5. Kubernetes Manifests

- [x] 5.1 Create coordinator Deployment manifest (env: `SQE_MODE=coordinator`, liveness/readiness probes on `/healthz` and `/readyz`)
- [x] 5.2 Create worker Deployment manifest (env: `SQE_MODE=worker`, `SQE_COORDINATOR_ADDR`)
- [x] 5.3 Create coordinator Service manifest (ClusterIP, Flight SQL port)

## 6. Testing

- [x] 6.1 Unit tests for mode selection logic (valid, invalid, missing `SQE_MODE`)
- [x] 6.2 Unit tests for config file loading and env var override precedence
- [x] 6.3 Integration test: `sqe-server` starts in coordinator mode and responds to `/healthz`
- [x] 6.4 Integration test: `sqe-cli -e "SELECT 1"` against running coordinator
- [x] 6.5 Docker build smoke test: verify image contains both binaries, runs as non-root
