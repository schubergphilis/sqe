# syntax=docker/dockerfile:1
#
# One image definition for local compose, data-platform quickstart/sqe, and
# aikido/kaniko. No cargo-chef, no sccache, no BuildKit cache mounts.
#
#   docker build -t sqe:latest .
#   docker build --target bench-runtime -t sqe-bench:latest .
#
# Final stage MUST stay named `runtime` (AIKIDO_DOCKER_TARGET in .gitlab-ci.yml).
# Runtime is Chainguard glibc-dynamic (UID 65532): glibc + libgcc + CA certs.
# TLS is rustls. Chosen over distroless/cc so the Aikido image gate
# (grype --fail-on high) stays green: distroless still ships unfixed debian
# libc criticals.

# Pin matches rust-toolchain.toml so the image and local/CI toolchains agree.
ARG RUST_VERSION=1.97.1

FROM rust:${RUST_VERSION}-bookworm AS builder

# libprotobuf-dev: well-known .proto includes for datafusion-substrait build.rs.
# clang/lld: faster link than the default binutils ld on amd64 and arm64.
RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake protobuf-compiler libprotobuf-dev libssl-dev pkg-config clang lld \
    && rm -rf /var/lib/apt/lists/*

ENV RUSTFLAGS="-C linker=clang -C link-arg=-fuse-ld=lld"
WORKDIR /build

COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
COPY vendor/ vendor/
# xtask is a workspace member; cargo metadata fails if it is missing.
COPY xtask/ xtask/

# --locked: same resolution as cargo-gate. --no-default-features: REST-only slim binary
# (Polaris/Nessie). Kitchen-sink backends: Dockerfile.full.
RUN cargo build --release --locked --no-default-features \
      --bin sqe-server --bin sqe-worker --bin sqe-cli --bin sqe-bench \
    && mkdir -p /build/out \
    && cp target/release/sqe-server target/release/sqe-worker \
         target/release/sqe-cli target/release/sqe-bench /build/out/

# ── Runtime (shared base) ─────────────────────────────────────
# Digest-pinned; Renovate bumps via the dockerfile manager.
FROM cgr.dev/chainguard/glibc-dynamic@sha256:eaec65b25f35619be16f4992e7bae1128eafcf63c114f2859b800a7020c1ef70 AS runtime-base

# Base image already runs as nonroot (65532). Explicit USER keeps trivy DS-0002 clean.
USER 65532

FROM busybox:1.38.0-uclibc@sha256:297dda192bda2157ddf40abb47a45a1090caff1864db9cfb9ce4b901ba318a3c AS healthcheck-bin

# Bench generator: same builder, smaller entrypoint image.
FROM runtime-base AS bench-runtime

LABEL org.opencontainers.image.title="sqe-bench" \
      org.opencontainers.image.description="TPC benchmark data generator for SQE"

COPY --from=builder /build/out/sqe-bench /usr/local/bin/

ENTRYPOINT ["/usr/local/bin/sqe-bench"]

# Default: server image (plain `docker build .`).
FROM runtime-base AS runtime

ARG VERSION=dev
ARG BUILD_DATE
ARG GIT_REVISION

LABEL org.opencontainers.image.title="sqe" \
      org.opencontainers.image.description="Sovereign Query Engine - distributed SQL over Apache Iceberg" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.revision="${GIT_REVISION}" \
      org.opencontainers.image.source="https://github.com/schuberg/sqe"

COPY --from=builder /build/out/sqe-server /build/out/sqe-worker \
    /build/out/sqe-cli /usr/local/bin/
COPY --from=healthcheck-bin /bin/wget /usr/local/bin/wget

EXPOSE 50051 50052 8080 9090 9091

HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
    CMD ["/usr/local/bin/wget", "-q", "-O", "/dev/null", "http://127.0.0.1:9091/healthz"]

ENTRYPOINT ["/usr/local/bin/sqe-server"]
