## Why

SQE needs a container packaging strategy for Kubernetes deployment. The coordinator and worker share ~80% of their code (DataFusion, Flight, auth, iceberg-rust), and operators need a CLI for interactive SQL and debugging. Shipping a single Docker image with a mode flag eliminates version skew between coordinator, worker, and CLI — and simplifies the CI/CD pipeline to one build, one scan, one promotion path.

## What Changes

- Introduce a unified `sqe-server` binary that runs as either coordinator or worker based on `SQE_MODE` environment variable / config file
- Introduce an `sqe-cli` binary for interactive SQL access (REPL over Flight SQL)
- Single multi-stage Dockerfile producing one image with both binaries
- Container entrypoint is `sqe-server`; `sqe-cli` is available in `$PATH` for `docker exec` / `kubectl exec` usage
- Cargo workspace configured with two `[[bin]]` targets in appropriate crates

## Capabilities

### New Capabilities
- `server-binary`: Unified sqe-server binary with coordinator/worker mode selection via env/config
- `cli-binary`: Interactive SQL CLI (sqe-cli) connecting over Flight SQL
- `container-image`: Multi-stage Dockerfile, image layout, entrypoint configuration, and K8s deployment patterns

### Modified Capabilities

## Impact

- **Cargo workspace**: Adds two binary targets (`sqe-server` in `sqe-coordinator` crate, `sqe-cli` as new crate or in `sqe-core`)
- **CI/CD**: Single Docker build pipeline (build → test → scan → push)
- **Kubernetes**: Coordinator and worker Deployments share the same image, differing only in `SQE_MODE` env var
- **Dependencies**: Adds `clap` for CLI argument parsing, `rustyline` or `reedline` for CLI REPL, `tonic`/`arrow-flight` for CLI Flight SQL client
