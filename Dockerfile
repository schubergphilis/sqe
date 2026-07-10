# syntax=docker/dockerfile:1
# ── Stage 1: Base builder with tools ──────────────────────────
# Use cargo-chef official image — cargo-chef pre-installed, avoids slow cargo install
FROM lukemathwalker/cargo-chef:latest-rust-bookworm AS chef

ARG TARGETARCH
ARG SCCACHE_VERSION=0.9.0

# Install build deps + download pre-compiled sccache binary (avoids ~10 min cargo install sccache)
# libprotobuf-dev is required in addition to protobuf-compiler: the protoc
# binary ships in protobuf-compiler, but the well-known type definitions
# (google/protobuf/any.proto, empty.proto, ...) ship in libprotobuf-dev under
# /usr/include. datafusion-substrait's build.rs imports them, so without it the
# deps layer fails with "google/protobuf/any.proto: File not found".
RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake protobuf-compiler libprotobuf-dev libssl-dev pkg-config clang lld curl && \
    rm -rf /var/lib/apt/lists/* && \
    case "$TARGETARCH" in \
        amd64) SCCACHE_ARCH=x86_64 ;; \
        arm64) SCCACHE_ARCH=aarch64 ;; \
        *) echo "unsupported arch: $TARGETARCH" && exit 1 ;; \
    esac && \
    curl -fsSL "https://github.com/mozilla/sccache/releases/download/v${SCCACHE_VERSION}/sccache-v${SCCACHE_VERSION}-${SCCACHE_ARCH}-unknown-linux-musl.tar.gz" \
    | tar xz --strip-components=1 -C /usr/local/bin \
        "sccache-v${SCCACHE_VERSION}-${SCCACHE_ARCH}-unknown-linux-musl/sccache"

# Use lld linker (faster than default ld, works on amd64 + aarch64)
ENV RUSTFLAGS="-C linker=clang -C link-arg=-fuse-ld=lld"
# Use sccache for compilation caching
ENV RUSTC_WRAPPER=sccache
# sccache config: local disk cache (in Docker BuildKit cache mount)
ENV SCCACHE_DIR=/sccache
ENV SCCACHE_CACHE_SIZE=2G

WORKDIR /build

# ── Stage 2: Compute recipe (changes only when Cargo.toml/lock change) ─
FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
COPY vendor/ vendor/
# xtask is listed as a workspace member in Cargo.toml; cargo metadata
# (which cargo-chef invokes under the hood) fails if it's absent.
COPY xtask/ xtask/
RUN cargo chef prepare --recipe-path recipe.json

# ── Stage 3: Build dependencies (cached unless recipe changes) ─
FROM chef AS deps
ARG TARGETARCH
COPY --from=planner /build/recipe.json recipe.json
# Vendored iceberg-rust crates (path dependencies in Cargo.toml)
COPY vendor/ vendor/
RUN --mount=type=cache,id=sqe-cargo-registry-${TARGETARCH},target=/usr/local/cargo/registry \
    --mount=type=cache,id=sqe-cargo-git-${TARGETARCH},target=/usr/local/cargo/git \
    --mount=type=cache,id=sqe-sccache-${TARGETARCH},target=/sccache \
    cargo chef cook --release --recipe-path recipe.json \
      --no-default-features \
      --package sqe-coordinator --package sqe-worker --package sqe-cli && \
    sccache --show-stats

# ── Stage 4: Build application (only workspace crates recompile) ─
FROM deps AS builder
ARG TARGETARCH
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
COPY vendor/ vendor/
# xtask must be present for workspace resolution, even though we don't
# build its binary here.
COPY xtask/ xtask/
RUN --mount=type=cache,id=sqe-cargo-registry-${TARGETARCH},target=/usr/local/cargo/registry \
    --mount=type=cache,id=sqe-cargo-git-${TARGETARCH},target=/usr/local/cargo/git \
    --mount=type=cache,id=sqe-sccache-${TARGETARCH},target=/sccache \
    --mount=type=cache,id=sqe-target-release-${TARGETARCH},target=/build/target,sharing=locked,from=deps,source=/build/target \
    cargo build --release --no-default-features \
      --bin sqe-server --bin sqe-worker --bin sqe-cli && \
    mkdir -p /build/out && \
    cp target/release/sqe-server target/release/sqe-worker target/release/sqe-cli /build/out/ && \
    sccache --show-stats

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

COPY --from=builder /build/out/ /usr/local/bin/

USER sqe
EXPOSE 50051 50052 8080 9090 9091

HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:9091/healthz || exit 1

ENTRYPOINT ["sqe-server"]
