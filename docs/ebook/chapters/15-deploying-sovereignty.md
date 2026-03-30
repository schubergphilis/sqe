# Deploying Sovereignty {#sec:deployment}

> A sovereign engine that's hard to deploy
> is just a sovereign engine that nobody runs.

The engine worked. On my laptop, in a terminal, with `cargo run` and a local Polaris instance, SQE parsed SQL, authenticated users, queried Iceberg tables, and returned Arrow batches over Flight SQL. Chapters 3 through 14 built all of that. But "works on my laptop" is not deployment. It is a demo.

The gap between a working binary and a running system is measured in container images, Helm charts, health probes, resource limits, upgrade strategies, and the dozen other things that have nothing to do with query execution but everything to do with whether anyone besides you will ever run the engine. This chapter closes that gap.


## The 2.3GB Problem

The first Docker image was a single-stage build. `FROM rust:latest`, copy the source, `cargo build --release`, done. It compiled. It ran. It was 2.3GB.

That size is not academic. A 2.3GB image means 45-second cold starts on a Kubernetes node that does not have it cached. It means saturating the pull bandwidth on a 1Gbps registry link during a rolling upgrade. It means CI pipelines that spend more time pushing images than running tests. And it means every developer on the team needs to pull 2.3GB the first time they run the test stack.

The Rust toolchain is the culprit. The `rust:latest` image carries the full compiler, standard library source, documentation, and every build tool. The compiled SQE binaries — `sqe-server`, `sqe-worker`, `sqe-cli` — total about 40MB. The other 2.26GB is build infrastructure that has no business being in a runtime image.

The fix is a multi-stage build. But a naive multi-stage build still has a problem: every code change triggers a full recompile of all 400+ dependencies. On a CI runner, that is 15 to 25 minutes. On a developer laptop behind a VPN, it is the moment when you go make coffee and consider whether you chose the right profession.


## Cargo-Chef and the Layer Cache

cargo-chef solves the dependency caching problem by separating the build into two phases: cook the dependencies, then build the application. The dependency layer only changes when `Cargo.toml` or `Cargo.lock` changes — which is infrequent compared to source code changes. Docker's layer cache keeps the cooked dependencies warm, and subsequent builds only recompile the workspace crates.

The Dockerfile has five stages:

```dockerfile
# ── Stage 1: Base builder with tools ──────────────────────────
FROM lukemathwalker/cargo-chef:latest-rust-bookworm AS chef

ARG TARGETARCH
ARG SCCACHE_VERSION=0.9.0

RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake protobuf-compiler libssl-dev pkg-config clang lld curl && \
    rm -rf /var/lib/apt/lists/* && \
    case "$TARGETARCH" in \
        amd64) SCCACHE_ARCH=x86_64 ;; \
        arm64) SCCACHE_ARCH=aarch64 ;; \
        *) echo "unsupported arch: $TARGETARCH" && exit 1 ;; \
    esac && \
    curl -fsSL "https://github.com/mozilla/sccache/releases/download/\
v${SCCACHE_VERSION}/sccache-v${SCCACHE_VERSION}-\
${SCCACHE_ARCH}-unknown-linux-musl.tar.gz" \
    | tar xz --strip-components=1 -C /usr/local/bin \
        "sccache-v${SCCACHE_VERSION}-${SCCACHE_ARCH}-unknown-linux-musl/sccache"

ENV RUSTFLAGS="-C linker=clang -C link-arg=-fuse-ld=lld"
ENV RUSTC_WRAPPER=sccache
ENV SCCACHE_DIR=/sccache
ENV SCCACHE_CACHE_SIZE=2G

WORKDIR /build
```

Stage 1 installs the build toolchain once. The `lld` linker is significantly faster than the default `ld` on both amd64 and aarch64. sccache adds a compilation cache that survives across Docker builds via BuildKit cache mounts. These two choices — fast linker, persistent compile cache — cut incremental build times from 15 minutes to under 3.

```dockerfile
# ── Stage 2: Compute recipe ──────────────────────────────────
FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
RUN cargo chef prepare --recipe-path recipe.json

# ── Stage 3: Build dependencies ──────────────────────────────
FROM chef AS deps
ARG TARGETARCH
COPY --from=planner /build/recipe.json recipe.json
RUN --mount=type=cache,id=sqe-cargo-registry-${TARGETARCH},target=/usr/local/cargo/registry \
    --mount=type=cache,id=sqe-cargo-git-${TARGETARCH},target=/usr/local/cargo/git \
    --mount=type=cache,id=sqe-sccache-${TARGETARCH},target=/sccache \
    cargo chef cook --release --recipe-path recipe.json && \
    sccache --show-stats
```

Stage 2 computes the "recipe" — a manifest of all dependencies without the actual source code. Stage 3 builds those dependencies using the recipe. The `--mount=type=cache` directives keep the Cargo registry, git checkouts, and sccache artifacts in BuildKit's persistent cache. When you change application code but not dependencies, Stage 3 is a complete cache hit. Zero work.

```dockerfile
# ── Stage 4: Build application ───────────────────────────────
FROM deps AS builder
ARG TARGETARCH
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
RUN --mount=type=cache,id=sqe-cargo-registry-${TARGETARCH},target=/usr/local/cargo/registry \
    --mount=type=cache,id=sqe-cargo-git-${TARGETARCH},target=/usr/local/cargo/git \
    --mount=type=cache,id=sqe-sccache-${TARGETARCH},target=/sccache \
    cargo build --release --bin sqe-server --bin sqe-worker --bin sqe-cli && \
    sccache --show-stats
```

Stage 4 copies the actual source and builds only the workspace crates against the pre-built dependencies. On a warm cache, this takes 30 to 90 seconds depending on how many crates changed.

```dockerfile
# ── Stage 5: Runtime image ───────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates libssl3 curl && \
    rm -rf /var/lib/apt/lists/* && \
    groupadd -r sqe && useradd -r -g sqe -u 1000 sqe

COPY --from=builder /build/target/release/sqe-server /usr/local/bin/
COPY --from=builder /build/target/release/sqe-worker /usr/local/bin/
COPY --from=builder /build/target/release/sqe-cli /usr/local/bin/

USER sqe
EXPOSE 50051 50052 8080 9090 9091

HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:9091/healthz || exit 1

ENTRYPOINT ["sqe-server"]
```

Stage 5 is the runtime. The `debian:bookworm-slim` base carries only what the binaries need: CA certificates for TLS to Polaris, libssl for HTTPS, and curl for the health check. The three binaries total about 40MB. The final image is 47MB.

From 2.3GB to 47MB. A 98% reduction. Cold pulls on a Kubernetes node take 3 seconds instead of 45.

We use `debian:bookworm-slim` instead of `scratch` for the runtime. The original plan was a fully static musl build running on scratch — zero runtime dependencies, zero attack surface. In practice, the Rust TLS ecosystem still has rough edges with musl static linking. OpenSSL bindings, which iceberg-rust pulls in transitively, resist static compilation on some architectures. The bookworm-slim base adds 25MB but eliminates a class of linking headaches that were consuming more debugging time than the size savings justified.

::: {.antipattern}
**Antipattern: scratch images for Rust services that use OpenSSL.** It sounds clean — no base OS, just your binary. But if any dependency in your tree links dynamically against libssl, the binary will fail with a cryptic "not found" error at startup. Either commit to rustls throughout your entire dependency tree, or accept the slim base. We chose the latter and moved on.
:::

The non-root user matters. SQE runs as UID 1000 in the `sqe` group. This is not security theater — Kubernetes `PodSecurityStandard` policies (and the older PodSecurityPolicy) can enforce non-root containers. Running as root means your deployment will be rejected by any cluster with basic security hygiene enabled.


## Helm Chart: Two Topologies, One Chart

![Deployment topology: coordinator and worker pods in Kubernetes with service routing, health probes, and Prometheus scraping](diagrams/rendered/15-deployment-topology.svg)

SQE has two deployment modes. In single-node (hybrid) mode, one process handles both coordination and execution. In distributed mode, a coordinator process handles SQL parsing, planning, and scheduling, while separate worker processes execute plan fragments. The Helm chart supports both topologies through a single `worker.enabled` toggle.

The chart has seven templates:

| Template | Purpose |
|---|---|
| `coordinator-deployment.yaml` | Coordinator Deployment (always present) |
| `worker-deployment.yaml` | Worker Deployment (conditional on `worker.enabled`) |
| `service.yaml` | ClusterIP Service for coordinator |
| `configmap.yaml` | Rendered `sqe.toml` configuration |
| `secret.yaml` | Optional inline secrets |
| `servicemonitor.yaml` | Prometheus Operator ServiceMonitor |
| `NOTES.txt` | Post-install connection instructions |

The coordinator is a Deployment, not a StatefulSet. It holds session state in memory, but that state is transient — if the coordinator restarts, clients reconnect and re-authenticate. There is nothing on disk that needs to survive a pod replacement.

Workers are also Deployments in the current chart. The original design called for a StatefulSet with stable network identities so the coordinator could address workers by predictable DNS names (`sqe-worker-0`, `sqe-worker-1`). In practice, the worker registration protocol — heartbeat-based, with the coordinator maintaining a live worker list — made stable identities unnecessary. Workers register themselves on startup. The coordinator discovers them. Stable DNS names would be nice for debugging but are not required for correctness.

The ConfigMap renders the TOML configuration directly from Helm values:

```yaml
data:
  sqe.toml: |
    [coordinator]
    flight_sql_port = 50051
    trino_http_port = 8080
    mode = "hybrid"
    worker_urls = ["http://sqe-worker-0.sqe-worker-headless:50052",
                   "http://sqe-worker-1.sqe-worker-headless:50052"]

    [worker]
    coordinator_url = "http://sqe-coordinator:50051"
    heartbeat_interval_secs = 5
    memory_limit = "8GB"
    spill_dir = "/tmp/sqe-spill"

    [auth]
    keycloak_url = "https://keycloak.example.com"
    realm = "iceberg"
    client_id = "sqe-client"
    token_refresh_buffer_secs = 60

    [catalog]
    polaris_url = "http://polaris:8181/api/catalog"
    warehouse = "iceberg"
    metadata_cache_ttl_secs = 30

    [storage]
    s3_endpoint = "http://s3:9000"
    s3_region = "us-east-1"
    s3_path_style = true

    [policy]
    engine = "passthrough"

    [metrics]
    prometheus_port = 9090
```

One decision worth explaining: the config checksum annotation on the pod template.

```yaml
annotations:
  checksum/config: {{ include (print .Template.BasePath "/configmap.yaml") . | sha256sum }}
```

This forces a pod restart when the ConfigMap content changes. Without it, a `helm upgrade` that only changes configuration would update the ConfigMap but leave the running pods on the old config. Kubernetes does not restart pods when a mounted ConfigMap changes. The checksum annotation converts a config change into a template change, which triggers a rolling restart.

Secrets follow the external pattern. The chart either references an existing Kubernetes Secret (`existingSecret`) or creates one from inline values. Sensitive fields — the OIDC client secret, S3 credentials — are injected as environment variables that override the TOML file. The TOML file never contains secrets. This keeps the ConfigMap safe to log, inspect, and version-control.

```yaml
existingSecret: ""

secrets: {}
  # SQE_AUTH__CLIENT_SECRET: "my-secret"
  # SQE_STORAGE__S3_ACCESS_KEY: "minioadmin"
  # SQE_STORAGE__S3_SECRET_KEY: "minioadmin"
```

The environment variable names follow a convention: `SQE_` prefix, double-underscore for section nesting. `SQE_AUTH__CLIENT_SECRET` maps to `[auth] client_secret` in the TOML. The engine's config loader checks environment variables after the TOML file, so env vars always win.


## Resource Requests That Make Sense

The coordinator and workers have fundamentally different resource profiles. Getting the requests and limits wrong means either wasted capacity or production outages.

The coordinator is I/O-bound. It parses SQL, resolves catalog metadata from Polaris (network I/O), builds logical plans, applies policy rewrites, splits plans into fragments, and dispatches them to workers. None of this is compute-intensive. The bottleneck is network round-trips to Polaris and the number of concurrent sessions held in memory.

Workers are compute-bound. They receive plan fragments, execute them against Parquet files (CPU-intensive columnar decoding, predicate evaluation, aggregation), and stream Arrow batches back. A worker processing a TPC-H Q1 aggregation will peg all available cores for the duration of the scan.

The default values reflect this:

```yaml
coordinator:
  resources:
    requests:
      memory: "512Mi"
      cpu: "500m"
    limits:
      memory: "2Gi"
      cpu: "2"

worker:
  resources:
    requests:
      memory: "1Gi"
      cpu: "1"
    limits:
      memory: "8Gi"
      cpu: "4"
```

The coordinator gets modest CPU (500m request, 2 cores limit) and moderate memory (512Mi to 2Gi). The memory covers session state, the metadata cache, the query history buffer, and the result cache. For most workloads, 2Gi is generous.

Workers get more of everything. The 1 core request ensures they are scheduled on nodes with real compute capacity, not squeezed onto a node that is already running 30 sidecar containers. The 4-core limit lets them burst for heavy scans. The 8Gi memory limit accommodates hash joins and sort buffers for complex queries, with spill-to-disk as the fallback when queries exceed available memory.

::: {.fieldreport}
**Field report:** Our first Helm deployment used `requests.memory: 512Mi` for workers. The first TPC-H query OOM'd. The second deployment used `8Gi`. The third deployment used `4Gi` with spill-to-disk. That was the right answer. The lesson: memory limits for query engines should be set based on your query complexity, not your binary size. The binary is 40MB. The sort buffer for a 200-million-row GROUP BY is 6GB.
:::

The request-to-limit ratio matters. A 1:8 ratio (512Mi request, 4Gi limit) means the scheduler will pack pods densely based on requests, but actual memory consumption can spike to 8x the request. On a node with 32Gi, Kubernetes will schedule 60 pods at 512Mi requests but only 4 can actually use 8Gi simultaneously. The first five queries run fine. The sixth triggers the OOM killer.

We settled on a 1:4 ratio for workers (1Gi request, 4Gi limit in moderate environments, 2Gi request, 8Gi limit in production). This gives enough breathing room for burst workloads without overcommitting the node to the point where the OOM killer becomes a scheduling strategy.

::: {.antipattern}
**Antipattern: setting requests equal to limits for query workers.** This guarantees your resource allocation (QoS class Guaranteed), but it also means you pay for peak capacity at all times. A worker sitting idle between queries still holds 8Gi of reserved memory. For bursty workloads — which describes every interactive SQL engine — Burstable QoS with a sensible request:limit ratio is the pragmatic choice.
:::


## Rolling Upgrades Without Dropping Queries

Deploying a new version of SQE should not kill in-flight queries. This sounds obvious. Getting it right is not.

The problem has two parts. First, the coordinator holds session state and in-progress query context. Replacing the coordinator pod means those sessions disappear. Second, workers may be executing plan fragments when a rolling update replaces them. Killing a worker mid-fragment means the coordinator receives a gRPC error instead of Arrow batches.

For the coordinator, the strategy is readiness-gate-driven. The coordinator exposes two health endpoints: `/healthz` (liveness — "am I running") and `/readyz` (readiness — "should I receive traffic"). During shutdown, the coordinator enters a draining state: it stops accepting new queries by returning `503` on `/readyz`, waits for in-flight queries to complete (with a configurable timeout), then exits cleanly. Kubernetes removes the pod from the Service endpoints as soon as the readiness probe fails, so new client connections go to the replacement pod while existing connections drain on the old one.

```yaml
livenessProbe:
  httpGet:
    path: /healthz
    port: health
  initialDelaySeconds: 5
  periodSeconds: 10
readinessProbe:
  httpGet:
    path: /readyz
    port: health
  initialDelaySeconds: 5
  periodSeconds: 5
```

The `terminationGracePeriodSeconds` (default 30 in Kubernetes) needs to be long enough for the longest expected query to finish. For interactive workloads with a 30-second timeout, the default is fine. For batch workloads running TPC-H Q9 (which can take minutes), increase it.

For workers, the mechanism is simpler because the coordinator manages retries. When a worker disappears — whether from a rolling update, a crash, or a node drain — the coordinator detects the missing heartbeat and reschedules the fragment to another worker. The query does not fail; it gets slower because one fragment is re-executed. The coordinator's fragment scheduler already handles this for crash recovery (Chapter 14). Rolling updates are just a graceful version of a crash.

A PodDisruptionBudget ensures Kubernetes does not drain too many workers simultaneously:

```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: sqe-worker-pdb
spec:
  minAvailable: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: sqe
      app.kubernetes.io/component: worker
```

`minAvailable: 1` means Kubernetes will never voluntarily evict the last running worker. During a rolling update of a 3-worker deployment, at most 2 workers are unavailable at any time. Combined with the coordinator's fragment retry, this means queries may slow down during an upgrade but they do not fail.

The one time it was not zero-downtime was instructive. We deployed a version that changed the protobuf schema for plan fragments. The new coordinator sent fragments that old workers could not deserialize. The workers returned errors. The coordinator retried on the same old workers. The queries failed. The fix was to upgrade workers first, then the coordinator. Workers are backward-compatible (they can handle old and new fragment formats). The coordinator is not (it sends the new format unconditionally). Upgrade order matters when the wire protocol changes.

::: {.fieldreport}
**Field report:** The protobuf-breaking upgrade taught us the rule: workers first, coordinator second. Workers understand old fragments. The coordinator sends new fragments. If you reverse the order, you have a window where the coordinator speaks a language no worker understands. We added this to the Helm chart's NOTES.txt and moved on.
:::


## The Lightweight Test Stack

Production runs Keycloak for OIDC, a real S3 service, and Polaris backed by a database. A developer running integration tests does not need any of that. They need the minimum viable infrastructure to execute a query against an Iceberg table.

The test stack is two containers: Polaris in in-memory mode and RustFS (a lightweight S3-compatible store written in Rust).

```yaml
# docker-compose.test.yml
services:
  polaris:
    image: apache/polaris:1.3.0-incubating
    environment:
      POLARIS_PERSISTENCE_TYPE: in-memory
      POLARIS_BOOTSTRAP_CREDENTIALS: "POLARIS,root,s3cr3t"
      POLARIS_PRODUCTION_READINESS_CHECKS_ENABLED: "false"
      QUARKUS_HTTP_PORT: 8181
      AWS_REGION: us-east-1
      AWS_ACCESS_KEY_ID: s3admin
      AWS_SECRET_ACCESS_KEY: s3admin
      QUARKUS_S3_ENDPOINT_OVERRIDE: http://rustfs:9000
      QUARKUS_S3_PATH_STYLE_ACCESS: "true"
    ports:
      - "18181:8181"
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8182/q/health"]
      interval: 5s
      timeout: 3s
      retries: 15
      start_period: 15s

  rustfs:
    image: rustfs/rustfs:latest
    environment:
      RUSTFS_ACCESS_KEY: s3admin
      RUSTFS_SECRET_KEY: s3admin
      RUSTFS_ADDRESS: ":9000"
      RUSTFS_VOLUMES: /data
    ports:
      - "19000:9000"
```

Polaris in-memory mode stores all catalog metadata in the JVM heap. No PostgreSQL. No MySQL. No JDBC configuration. It starts in 8 seconds and provides the full Iceberg REST catalog API. The `POLARIS_BOOTSTRAP_CREDENTIALS` environment variable creates a root principal automatically — no manual setup required.

RustFS replaces MinIO for local development. It is a single binary that speaks S3v4 auth, supports the operations Iceberg needs (PutObject, GetObject, ListObjectsV2, DeleteObject), and starts in under a second. The total memory footprint for both containers is about 400MB. Compare this to a full quickstart stack with Keycloak, PostgreSQL, and MinIO, which consumes 2GB before you run a single query.

Port offsets keep the test stack from colliding with any production services or other development environments running on the same machine. Polaris on 18181 instead of 8181. RustFS on 19000 instead of 9000. The bootstrap script knows these offsets and configures everything accordingly.

The bootstrap script (`scripts/bootstrap-test.sh`) is idempotent. It waits for Polaris and RustFS to be healthy, creates the S3 bucket, obtains an OAuth2 token from Polaris, creates the warehouse catalog, grants access, and creates the default namespace. Run it once or ten times — the result is the same.

```bash
docker compose -f docker-compose.test.yml up -d
./scripts/bootstrap-test.sh
source tests/.test-env && cargo test -p sqe-coordinator --test integration_test -- --ignored
```

Three commands. From zero to running integration tests. No cloud account. No VPN. No configuration file you need to copy from a wiki page that was last updated in 2023.

::: {.sovereignty}
**Sovereignty principle:** If your test infrastructure requires a cloud account, your development velocity is gated by your cloud provider. The Polaris + RustFS stack runs entirely on localhost. A new team member can run the full integration test suite on their first day, on an airplane, with no internet connection. That is sovereignty in development workflow.
:::


## The Distributed Test Stack

The test stack validates single-node behavior. The distributed test stack validates the coordinator-worker protocol, fragment distribution, and system tables — the full distributed topology from Chapters 12 through 14.

Running the full distributed stack on localhost is not a luxury. It is the only way to catch the class of bugs that live between processes — serialization mismatches, heartbeat timeouts, fragment routing errors — before they reach a shared environment where six other people are trying to get their own work done.

It extends the test stack with three additional containers: one coordinator and two workers.

```yaml
# docker-compose.distributed.yml
services:
  coordinator:
    build: .
    entrypoint: ["sqe-server", "--config", "/config/coordinator.toml"]
    volumes:
      - ./tests/distributed/coordinator.toml:/config/coordinator.toml:ro
    ports:
      - "60051:50051"   # Flight SQL
      - "28080:8080"    # Trino HTTP
      - "29090:9090"    # Prometheus metrics
    environment:
      RUST_LOG: sqe=info,warn
    depends_on:
      polaris:
        condition: service_healthy
      rustfs:
        condition: service_started

  worker-1:
    build: .
    entrypoint: ["sqe-worker", "/config/worker.toml"]
    volumes:
      - ./tests/distributed/worker.toml:/config/worker.toml:ro
    ports:
      - "60061:50052"
      - "29091:9091"
    environment:
      RUST_LOG: sqe=info,warn
    depends_on:
      - coordinator

  worker-2:
    build: .
    entrypoint: ["sqe-worker", "/config/worker.toml"]
    volumes:
      - ./tests/distributed/worker.toml:/config/worker.toml:ro
    ports:
      - "60062:50052"
      - "29092:9091"
    environment:
      RUST_LOG: sqe=info,warn
    depends_on:
      - coordinator
```

The coordinator config lists both workers explicitly:

```toml
[coordinator]
flight_sql_port = 50051
trino_http_port = 8080
worker_urls = ["http://worker-1:50052", "http://worker-2:50052"]
```

The worker config points back to the coordinator for heartbeat registration:

```toml
[worker]
flight_port = 50052
coordinator_url = "http://coordinator:50051"
heartbeat_interval_secs = 5
memory_limit = "512MB"
```

Bringing up the distributed stack composes both files:

```bash
docker compose -f docker-compose.test.yml \
               -f docker-compose.distributed.yml up --build -d
./scripts/bootstrap-distributed.sh
./scripts/distributed-test.sh
```

The distributed test script runs 14 assertions: basic connectivity, system.runtime.nodes (verifying the coordinator sees both workers), query history, catalog metadata, table creation, distributed execution, the result cache, Trino HTTP compatibility, and information_schema. It is not a benchmark. It is a smoke test that the distributed topology works end-to-end.

The concurrent load test script (`scripts/concurrent-test.sh`) goes further. It launches N parallel clients — default 10, configurable up to 50 or more — each running queries against the coordinator simultaneously. It measures per-query latency, tracks pass/fail rates, and queries `system.runtime.tasks` afterward to verify that work was actually distributed across both workers.

```bash
./scripts/concurrent-test.sh 20 mixed
```

This was the test that broke the distributed execution the first time. Fifty concurrent clients against two workers with 512MB memory each. The coordinator queued fragments faster than the workers could execute them. Workers OOM'd. The coordinator retried on dead workers. The system entered a failure spiral. The fix was twofold: memory-aware scheduling (the coordinator checks worker memory before dispatching) and backpressure (the coordinator limits the number of in-flight fragments per worker). Chapter 14 covers the debugging story. The deployment lesson was simpler: do not ship a default worker memory limit that cannot survive your own load test.


## The Production Compose Overlay

The repository includes a docker-compose overlay (`deploy/docker-compose.sqe.yml`) that drops SQE into an existing infrastructure stack, replacing Trino. This is the "try SQE in your current environment" path.

```yaml
services:
  sqe:
    build:
      context: ../../sql-engine
      dockerfile: Dockerfile
    command: ["--config", "/etc/sqe/sqe.toml"]
    ports:
      - "50051:50051"    # Flight SQL
    environment:
      SQE_AUTH__KEYCLOAK_URL: "https://auth.local"
      SQE_AUTH__REALM: "iceberg"
      SQE_CATALOG__POLARIS_URL: "http://polaris:8181/api/catalog"
      SQE_STORAGE__S3_ENDPOINT: "http://s3:9000"
      SQE_METRICS__OTLP_ENDPOINT: "http://jaeger:4317"
    mem_limit: 2g
    cpus: 2.0
    security_opt:
      - no-new-privileges:true
    cap_drop:
      - ALL
    depends_on:
      polaris:
        condition: service_healthy
      keycloak:
        condition: service_healthy

  # Override backend to point at SQE instead of Trino
  backend:
    environment:
      TRINO_HOST: sqe
      TRINO_PORT: "8080"

  # Scale Trino to zero
  trino:
    deploy:
      replicas: 0
```

The `security_opt` and `cap_drop` settings enforce container hardening. `no-new-privileges` prevents privilege escalation through setuid binaries. `cap_drop: ALL` removes all Linux capabilities. The SQE binary does not need any capabilities — it binds to non-privileged ports (all above 1024), runs as a non-root user, and performs no privileged system calls.

The Trino replacement is clean. The backend service points at SQE's Trino-compatible HTTP endpoint on port 8080. SQE speaks enough of the Trino wire protocol for existing tools — dashboards, notebooks, scheduled reports — to connect without modification. Trino is scaled to zero replicas — not removed from the compose file, so reverting is a one-line change. This matters more than it looks. Adoption that cannot be reversed will not be attempted. The compose overlay makes the decision reversible, and reversible decisions get approved faster.


## The Service Topology

Inside Kubernetes, the network topology needs to reflect the trust model. The coordinator is the only component that clients talk to. Workers are internal execution resources. Making this boundary explicit in the Service definitions prevents an entire category of misconfiguration.

The coordinator Service exposes three ports:

```yaml
spec:
  type: ClusterIP
  ports:
    - name: flight-sql
      port: 50051
      targetPort: flight-sql
    - name: trino-http
      port: 8080
      targetPort: trino-http
    - name: metrics
      port: 9090
      targetPort: metrics
```

Flight SQL (gRPC on 50051) is the primary protocol for JDBC clients, Python via ADBC, and dbt-sqe. Trino HTTP (8080) provides backward compatibility for tools that speak the Trino wire protocol. Metrics (9090) is the Prometheus scrape target.

Workers do not have a public Service. The coordinator talks to workers directly via their pod IPs (resolved through the headless service or the worker_urls configuration). This is intentional. Workers should never be addressable from outside the cluster. They execute plan fragments with the user's bearer token — exposing them would create a vector for token replay attacks.

The ServiceMonitor integration is optional and conditional:

```yaml
{{- if .Values.serviceMonitor.enabled }}
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: {{ include "sqe.fullname" . }}
spec:
  selector:
    matchLabels:
      {{- include "sqe.selectorLabels" . | nindent 6 }}
  endpoints:
    - port: metrics
      interval: 30s
{{- end }}
```

If the cluster runs the Prometheus Operator, enabling the ServiceMonitor gives you automatic scrape configuration. If it does not, the metrics port is still exposed and can be scraped by any Prometheus instance using static target configuration.


## What Happens on Day Two

Day one is deploying the chart. Day two is the `helm upgrade` three weeks later when you have users running queries and expectations.

The difference between day one and day two is accountability. On day one, if something breaks, you fix it and nobody noticed. On day two, a broken upgrade means an analyst cannot run a report, a dbt pipeline misses its SLA, and someone posts in Slack asking why the query engine is down. Day-two operations have to be boring. Predictable. Unremarkable.

The upgrade sequence we settled on:

1. Upgrade workers first. The new worker image registers with the coordinator using the same heartbeat protocol. Old and new workers coexist. The coordinator dispatches fragments to whatever workers are healthy.

2. Upgrade the coordinator. The old coordinator drains in-flight queries. Kubernetes replaces the pod. The new coordinator starts, re-establishes worker connections, and begins accepting queries. There is a brief window (5-15 seconds) where no new queries are accepted. Existing JDBC connections retry automatically because Flight SQL sits on gRPC, and gRPC clients handle connection resets.

3. Verify with `system.runtime.nodes`. After the upgrade, query the system table to confirm the coordinator sees all workers and all nodes report the new version.

```sql
SELECT node_id, state, version FROM system.runtime.nodes;
```

If this shows two workers in `ACTIVE` state and the coordinator in `READY` state, the upgrade succeeded. If a worker is stuck in `STARTING`, it cannot reach Polaris or the coordinator. Check the worker logs and the network policy.

The Helm chart's NOTES.txt prints connection instructions after every install and upgrade:

```
SQE has been deployed!

Coordinator: sqe-coordinator
Workers:     2 replicas

Connect via Flight SQL:
  kubectl port-forward svc/sqe-coordinator 50051:50051
  sqe-cli --host localhost --port 50051

Connect via Trino HTTP:
  kubectl port-forward svc/sqe-coordinator 8080:8080
```

It is not clever. It does not need to be. The person running `helm upgrade` at 11pm during a maintenance window needs exactly this: the service name, the port, and how to verify it worked.


## The Deployment as a Product Decision

Every technical choice in this chapter — the multi-stage build, the Helm topology, the test stack, the resource defaults — is a product decision disguised as infrastructure.

A 47MB image means a platform team can adopt SQE without filing a ticket to increase their registry quota. A two-container test stack means a data engineer can run integration tests without a cloud account. A Helm chart with sensible defaults means the first deployment is `helm install sqe ./deploy/helm/sqe` — not a 40-page runbook.

The Trino replacement overlay means adoption is not all-or-nothing. Run SQE alongside Trino. Route one workload to SQE. Compare. If it works, route more. If it does not, scale SQE to zero and Trino back to one. The rollback path is a single line in a compose file.

These choices are sovereignty choices. An engine that requires a dedicated infrastructure team to deploy is an engine that is controlled by whoever has access to that team's calendar. An engine that a single engineer can deploy, test, and upgrade — with `docker compose up` or `helm install` — is an engine that the people who need it can actually run.

The best query engine is the one people actually run. And people run engines they can deploy, test, break, fix, and upgrade without calling a meeting first.

::: {.ailog}
**AI Logbook:** The AI wrote the five-stage Dockerfile with cargo-chef, sccache, and lld — reducing the image from 2.3GB to 47MB — and generated all seven Helm chart templates including the config checksum annotation trick for triggering rolling restarts on ConfigMap changes. The human decided the deployment topology (Deployment not StatefulSet, workers-first upgrade order) and the port allocation scheme. The `no-new-privileges` and `cap_drop: ALL` security hardening on the production compose overlay was the human's specification; the AI applied it without understanding why it mattered.
:::

A sovereign engine that is hard to deploy is just a sovereign engine that nobody runs.
