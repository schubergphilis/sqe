# ── Stage 1: Generate dependency recipe ───────────────────────
FROM rust:bookworm AS chef

RUN cargo install cargo-chef --locked && \
    apt-get update && apt-get install -y --no-install-recommends \
    cmake protobuf-compiler libssl-dev pkg-config && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /build

# ── Stage 2: Compute recipe (changes only when Cargo.toml/lock change) ─
FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
RUN cargo chef prepare --recipe-path recipe.json

# ── Stage 3: Build dependencies (cached unless recipe changes) ─
FROM chef AS deps
COPY --from=planner /build/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# ── Stage 4: Build application (only workspace crates recompile) ─
FROM deps AS builder
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
RUN cargo build --release --bin sqe-server --bin sqe-cli

# ── Stage 5: Runtime image ────────────────────────────────────
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
    ca-certificates libssl3 curl && \
    rm -rf /var/lib/apt/lists/* && \
    groupadd -r sqe && useradd -r -g sqe -u 1000 sqe

COPY --from=builder /build/target/release/sqe-server /usr/local/bin/
COPY --from=builder /build/target/release/sqe-cli /usr/local/bin/

USER sqe
EXPOSE 50051 50052 8080 9090 9091

HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:9091/healthz || exit 1

ENTRYPOINT ["sqe-server"]
