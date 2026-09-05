# 809: Deployment artifacts disagree with each other and with the binaries

- Scope: Dockerfile, Dockerfile.full, Dockerfile.bench, deploy/k8s, deploy/helm, deploy/docker-compose.sqe.yml, docker-compose.*.yml, docs/site production guide
- Severity: medium
- Category: operability
- Effort: M
- Status: open
- Issue: #641

## Problem

There are three Dockerfiles, twelve root `docker-compose.*.yml` files, a
`deploy/docker-compose.sqe.yml`, raw manifests under `deploy/k8s/`, and a
Helm chart. They drift in ways an operator will hit.

1. **Raw k8s manifests are dead and wrong.** `deploy/k8s/coordinator.yaml:22-27`
   and `worker.yaml:22-27` use `image: sqe:latest`, no `--mode`, and set
   `SQE_MODE` and `SQE_COORDINATOR_ADDR` env vars. `SQE_MODE` is honoured
   (`crates/sqe-coordinator/src/mode.rs:58`); `SQE_COORDINATOR_ADDR` is read
   nowhere in `crates/`. `grep -rn deploy/k8s README.md Makefile docs/site`
   returns nothing: no document points at these files.

2. **The example compose uses the legacy config surface and insecure defaults.**
   `deploy/docker-compose.sqe.yml:22-26` sets `SQE_AUTH__KEYCLOAK_URL`,
   `SQE_AUTH__REALM`, `SQE_AUTH__CLIENT_ID`, `SQE_AUTH__CLIENT_SECRET`
   (the flat fields that the modern `[[auth.providers]]` model supersedes),
   `SQE_AUTH__SSL_VERIFICATION: "false"`, and `s3admin`/`s3admin` storage
   credentials as fallbacks (`:34-35`). The June review asked for both of the
   latter to be removed from examples; `sqe.toml.example:101` was fixed, this
   file was not.

3. **Two image builds with different feature sets and toolchains.**
   `Dockerfile:87` builds with `--no-default-features` and cargo-chef +
   sccache; `Dockerfile.full:26-27` builds `--features sqe-catalog/full-backends`
   with a plain `cargo build`. The `Makefile:39` default is `Dockerfile.full`;
   CI (`.gitlab-ci.yml:64-101`) builds `Dockerfile`. The production guide
   (`docs/site/book/src/operations/production-guide.md:24`) says the slim
   image is "REST catalog + SigV4 (`rest` + `rest-sigv4`)", but
   `crates/sqe-catalog/Cargo.toml` `default = ["rest", "rest-sigv4",
   "hive-external", "hadoop", "sql-sqlite"]`, and `--no-default-features` at
   the workspace level does not turn off a dependency's defaults, so the
   slim image ships hadoop, hive-external and sqlx/SQLite as well.

4. **Chart version is frozen.** `deploy/helm/sqe/Chart.yaml` has
   `version: 0.1.0`, `appVersion: "0.37.0"`; the workspace is 0.37.0. Every
   release ships a chart whose own version never changes, so `helm upgrade`
   cannot tell chart changes apart.

5. **Health, limits and secrets are inconsistent.** Helm coordinator and
   worker have liveness/readiness probes on `/healthz` and `/readyz`
   (`coordinator-deployment.yaml:100-111`); `docker-compose.distributed.yml`
   has no `healthcheck:` at all for coordinator or workers and no memory
   limits, while `docker-compose.observability.yml:27,43` sets `mem_limit`
   for side-cars only. The standalone `sqe-worker` binary that compose runs
   has no `/healthz` route (see 801), so a healthcheck could not be added
   without changing the binary.

## Why it matters

- An operator copying `deploy/docker-compose.sqe.yml` or `deploy/k8s/` gets
  a configuration that either does not apply (unknown env var) or is
  insecure by default, and the repo gives no signal which artifact is
  canonical.
- Binary size and attack surface of the "slim" image are misdescribed in
  the published production guide.
- Compose-based benchmarks and parity runs (the 2w/4w files) run workers
  without health checks or limits, so a wedged worker is invisible until a
  query hangs.

## Proposed change

1. Delete `deploy/k8s/` and `deploy/docker-compose.sqe.yml`; point the
   README at Helm and `quickstart/`.
2. Make `Dockerfile` the only server image and have `Dockerfile.full`
   become a build-arg (`--build-arg CATALOG_FEATURES=sqe-catalog/full-backends`)
   on the same file; delete `Dockerfile.full`. Keep `Dockerfile.bench` only
   if the `bench-runtime` target in `Dockerfile:103-110` cannot replace it
   (it appears to already).
3. Fix the slim build to mean what the guide says: pass
   `--no-default-features --features sqe-catalog/rest,sqe-catalog/rest-sigv4`
   explicitly to the coordinator and worker packages, or correct the guide
   to list the real default set.
4. Bump `Chart.yaml` `version` in `scripts/release.sh` alongside `appVersion`.
5. Add `healthcheck:` and `mem_limit:` to coordinator and worker services in
   `docker-compose.distributed.yml`, `bench-2w.yml`, `bench-4w.yml`,
   `parity.yml` (after 801 gives the worker a health route).
6. Add a CI job that `helm template`s `values.yaml` and
   `values-production.yaml` and greps the rendered manifests for the env
   var names against the allowlist from 802.

## Acceptance criteria

- `ls deploy/` shows `helm/` and `observability/` only.
- `docker build -t sqe .` and `docker build --build-arg CATALOG_FEATURES=... -t sqe-full .` both succeed from one Dockerfile.
- `cargo tree -p sqe-coordinator -e features` for the slim build contains no `sqlx`; or the production guide lists sqlx as included.
- `helm lint` passes and `Chart.yaml` version equals the workspace version after `scripts/release.sh`.
- `docker compose -f docker-compose.distributed.yml ps` shows `(healthy)` for coordinator and both workers.

## Related

- 801 (entry points), 802 (env surface).
- July 2026 production readiness review, P1-8 is addressed by `values-production.yaml`; this issue is the remaining artifact set.
- June 2026 codebase analysis, Improvements 10 and 11 (partially done: TOML example fixed, compose file not).
