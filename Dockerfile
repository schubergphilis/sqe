# ── Build stage ──────────────────────────────────────────────
FROM rust:bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake protobuf-compiler libssl-dev pkg-config && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /build

# ── Dependency caching layer ────────────────────────────────
# Copy only manifests first so dependency compilation is cached
COPY Cargo.toml Cargo.lock ./
COPY crates/sqe-core/Cargo.toml crates/sqe-core/Cargo.toml
COPY crates/sqe-auth/Cargo.toml crates/sqe-auth/Cargo.toml
COPY crates/sqe-catalog/Cargo.toml crates/sqe-catalog/Cargo.toml
COPY crates/sqe-sql/Cargo.toml crates/sqe-sql/Cargo.toml
COPY crates/sqe-policy/Cargo.toml crates/sqe-policy/Cargo.toml
COPY crates/sqe-planner/Cargo.toml crates/sqe-planner/Cargo.toml
COPY crates/sqe-coordinator/Cargo.toml crates/sqe-coordinator/Cargo.toml
COPY crates/sqe-worker/Cargo.toml crates/sqe-worker/Cargo.toml
COPY crates/sqe-trino-compat/Cargo.toml crates/sqe-trino-compat/Cargo.toml
COPY crates/sqe-metrics/Cargo.toml crates/sqe-metrics/Cargo.toml
COPY crates/sqe-cli/Cargo.toml crates/sqe-cli/Cargo.toml

# Create dummy source files so cargo can resolve the workspace and cache deps
RUN find crates -name "Cargo.toml" -exec sh -c ' \
    dir=$(dirname "$1"); \
    mkdir -p "$dir/src" "$dir/src/bin"; \
    echo "fn main() {}" > "$dir/src/main.rs"; \
    echo "" > "$dir/src/lib.rs"; \
    ' _ {} \;
RUN cargo build --release --bin sqe-server --bin sqe-cli 2>/dev/null || true

# ── Full build ──────────────────────────────────────────────
# Copy real sources — only recompiles workspace crates, not deps
COPY crates/ crates/
RUN touch crates/*/src/*.rs && \
    cargo build --release --bin sqe-server --bin sqe-cli

# ── Runtime image ───────────────────────────────────────────
FROM debian:bookworm-slim

ARG VERSION=dev
ARG BUILD_DATE
ARG GIT_REVISION

LABEL org.opencontainers.image.title="sqe" \
      org.opencontainers.image.description="Sovereign Query Engine — distributed SQL over Apache Iceberg" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.revision="${GIT_REVISION}" \
      org.opencontainers.image.source="https://github.com/schuberg/sqe"

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates libssl3 && \
    rm -rf /var/lib/apt/lists/* && \
    groupadd -r sqe && useradd -r -g sqe -u 1000 sqe

COPY --from=builder /build/target/release/sqe-server /usr/local/bin/
COPY --from=builder /build/target/release/sqe-cli /usr/local/bin/

USER sqe
EXPOSE 50051 50052 8080 9090 9091

ENTRYPOINT ["sqe-server"]
