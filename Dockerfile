# syntax=docker/dockerfile:1
# Runtime image for aikido-build (kaniko) and local `docker build`.
# Final stage MUST remain `AS runtime` (see AIKIDO_DOCKER_TARGET in .gitlab-ci.yml).
# Kaniko ignores BuildKit cache mounts below; cargo-chef layers still cache deps.
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
# Shared by every SQE Docker target in the active BuildKit builder.  The
# DataFusion/Arrow graph can evict useful objects quickly from the old 2 GiB
# limit when switching between the server and benchmark binaries.
ENV SCCACHE_CACHE_SIZE=5G

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
ARG CARGO_PROFILE=release
COPY --from=planner /build/recipe.json recipe.json
# Vendored iceberg-rust crates (path dependencies in Cargo.toml)
COPY vendor/ vendor/
# `sharing=locked` on the registry/git/sccache mounts: their cache `id`s are
# shared verbatim with Dockerfile.full and the legacy Dockerfile.bench, so builds
# running concurrently (e.g. building both images in parallel) race on the
# SAME cache mount. Default (unlocked) sharing lets both writers unpack into
# it at once, which corrupts the registry -- symptom: `failed to unpack
# package ...`, `.cargo-ok: File exists (os error 17)`. Locked serializes
# access instead of corrupting it; a concurrent build waits, it doesn't fail.
RUN --mount=type=cache,id=sqe-cargo-registry-${TARGETARCH},sharing=locked,target=/usr/local/cargo/registry \
    --mount=type=cache,id=sqe-cargo-git-${TARGETARCH},sharing=locked,target=/usr/local/cargo/git \
    --mount=type=cache,id=sqe-sccache-${TARGETARCH},sharing=locked,target=/sccache \
    cargo chef cook --profile "${CARGO_PROFILE}" --recipe-path recipe.json \
      --no-default-features \
      --package sqe-coordinator --package sqe-worker --package sqe-cli \
      --package sqe-bench && \
    sccache --show-stats

# ── Stage 4: Build application (only workspace crates recompile) ─
FROM deps AS builder
ARG TARGETARCH
ARG CARGO_PROFILE=release
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
COPY vendor/ vendor/
# xtask must be present for workspace resolution, even though we don't
# build its binary here.
COPY xtask/ xtask/
RUN --mount=type=cache,id=sqe-cargo-registry-${TARGETARCH},sharing=locked,target=/usr/local/cargo/registry \
    --mount=type=cache,id=sqe-cargo-git-${TARGETARCH},sharing=locked,target=/usr/local/cargo/git \
    --mount=type=cache,id=sqe-sccache-${TARGETARCH},sharing=locked,target=/sccache \
    --mount=type=cache,id=sqe-target-${CARGO_PROFILE}-${TARGETARCH},target=/build/target,sharing=locked,from=deps,source=/build/target \
    cargo build --profile "${CARGO_PROFILE}" --no-default-features \
      --bin sqe-server --bin sqe-worker --bin sqe-cli --bin sqe-bench && \
    mkdir -p /build/out && \
    cp "target/${CARGO_PROFILE}/sqe-server" "target/${CARGO_PROFILE}/sqe-worker" \
      "target/${CARGO_PROFILE}/sqe-cli" "target/${CARGO_PROFILE}/sqe-bench" /build/out/ && \
    sccache --show-stats

# ── Stage 5: Shared runtime base ──────────────────────────────
# distroless/cc: glibc + libgcc + CA certs, non-root UID 65532, no shell,
# no package manager. SQE links only libc/libm/libgcc (rustls, not OpenSSL),
# so debian:bookworm-slim + apt packages were pure CVE surface.
# Digest-pinned; Renovate bumps via the dockerfile manager.
FROM gcr.io/distroless/cc-debian12:nonroot@sha256:fccdbb0a547c14e23fcf4ce8ad62ca5d43b4faae8d22cd292f490fef9946c96e AS runtime-base

# Static busybox wget for image/compose HEALTHCHECK only. No shell, no curl,
# no libssl. uclibc busybox is a single static binary (0 OS CVEs at pin time).
FROM busybox:1.37.0-uclibc@sha256:8d7b1636e974e0adfd8d945955fca609304f0a56c18799dfd032d6e661382d84 AS healthcheck-bin

# The two runtime targets below intentionally share the expensive `builder`
# stage. `docker compose build` therefore compiles and links all four binaries
# once, while retaining distinct image entrypoints for the long-running server
# and the one-shot benchmark generator.
FROM runtime-base AS bench-runtime

LABEL org.opencontainers.image.title="sqe-bench" \
      org.opencontainers.image.description="TPC benchmark data generator for SQE"

COPY --from=builder /build/out/sqe-bench /usr/local/bin/

ENTRYPOINT ["/usr/local/bin/sqe-bench"]

# Keep the server runtime last so a plain `docker build .` remains compatible.
FROM runtime-base AS runtime

ARG VERSION=dev
ARG BUILD_DATE
ARG GIT_REVISION

LABEL org.opencontainers.image.title="sqe" \
      org.opencontainers.image.description="Sovereign Query Engine — distributed SQL over Apache Iceberg" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.revision="${GIT_REVISION}" \
      org.opencontainers.image.source="https://github.com/schuberg/sqe"

COPY --from=builder /build/out/sqe-server /build/out/sqe-worker \
    /build/out/sqe-cli /usr/local/bin/
# wget is only for HEALTHCHECK / compose probes; not on the server hot path.
COPY --from=healthcheck-bin /bin/wget /usr/local/bin/wget

EXPOSE 50051 50052 8080 9090 9091

# No shell in distroless: exec-form only. K8s uses HTTP probes and ignores this.
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
    CMD ["/usr/local/bin/wget", "-q", "-O", "/dev/null", "http://127.0.0.1:9091/healthz"]

ENTRYPOINT ["/usr/local/bin/sqe-server"]
