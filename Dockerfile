# syntax=docker/dockerfile:1
#
# Shared fast build for local Compose, data-platform quickstart/sqe, and CI.
# Dependencies are isolated with cargo-chef and compiler/target state survives
# source edits in locked BuildKit caches. Runtime and bench-runtime consume the
# same builder, so building both targets compiles the Rust graph only once.
#
#   docker build -t sqe:latest .
#   docker build --build-arg CARGO_PROFILE=dev-release -t sqe:dev .
#   docker build --target bench-runtime -t sqe-bench:latest .
#
# Final stage MUST stay named `runtime` (AIKIDO_DOCKER_TARGET in .gitlab-ci.yml).
# Runtime stays Chainguard glibc-dynamic (UID 65532), with no shell.

# Reuse the packaged cargo-chef binary without surrendering the repository's
# pinned Rust toolchain to whichever compiler the cargo-chef image currently
# carries.
FROM lukemathwalker/cargo-chef:latest-rust-bookworm@sha256:2ee6e8edf0b91b5a7295071299596601710bccb243bd9735e0b9abf6e114582b AS cargo-chef-bin

# Renovate's "container images" group bumps tag and digest together; the rust tag must match rust-toolchain.toml.
FROM rust:1.98.0-bookworm@sha256:82150a52ec202c1b14d7817e14516c392bb7f5cfebd88f1ed531cb37ebd39922 AS chef

ARG TARGETARCH
ARG SCCACHE_VERSION=0.9.0
# Upstream release .sha256 files for SCCACHE_VERSION; bump together.
ARG SCCACHE_SHA256_X86_64=f29522f9aa70aa929b7e76b6d4a1286196b17668ea753e5687899a8f7e4b9d3e
ARG SCCACHE_SHA256_AARCH64=d0122fbeda107eff00a6401d6e3085fd1e361813821a05bcd224fc1d7071ecdf

COPY --from=cargo-chef-bin /usr/local/cargo/bin/cargo-chef /usr/local/cargo/bin/cargo-chef

# libprotobuf-dev supplies google/protobuf/*.proto for datafusion-substrait.
# lld materially shortens the final coordinator link. The prebuilt sccache
# binary avoids spending several minutes compiling the cache itself.
RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake protobuf-compiler libprotobuf-dev libssl-dev pkg-config clang lld curl \
    && rm -rf /var/lib/apt/lists/* \
    && case "$TARGETARCH" in \
         amd64) SCCACHE_ARCH=x86_64 SCCACHE_SHA256=$SCCACHE_SHA256_X86_64 ;; \
         arm64) SCCACHE_ARCH=aarch64 SCCACHE_SHA256=$SCCACHE_SHA256_AARCH64 ;; \
         *) echo "unsupported arch: $TARGETARCH" >&2; exit 1 ;; \
       esac \
    && curl -fsSL -o /tmp/sccache.tar.gz \
      "https://github.com/mozilla/sccache/releases/download/v${SCCACHE_VERSION}/sccache-v${SCCACHE_VERSION}-${SCCACHE_ARCH}-unknown-linux-musl.tar.gz" \
    && echo "${SCCACHE_SHA256}  /tmp/sccache.tar.gz" | sha256sum -c - \
    && tar xzf /tmp/sccache.tar.gz --strip-components=1 -C /usr/local/bin \
        "sccache-v${SCCACHE_VERSION}-${SCCACHE_ARCH}-unknown-linux-musl/sccache" \
    && rm /tmp/sccache.tar.gz

ENV RUSTFLAGS="-C linker=clang -C link-arg=-fuse-ld=lld" \
    RUSTC_WRAPPER=sccache \
    SCCACHE_DIR=/sccache \
    SCCACHE_CACHE_SIZE=5G

WORKDIR /build

# Compute a dependency-only recipe. Source edits rerun the cheap planner but
# leave the expensive cook layer/cache reusable while manifests stay stable.
FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
COPY vendor/ vendor/
COPY xtask/ xtask/
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS deps
ARG TARGETARCH
ARG CARGO_PROFILE=release
# Empty = slim (catalog crate defaults). Set to `sqe-catalog/full-backends`
# for Glue/HMS/JDBC/Hadoop: `docker build --build-arg CATALOG_FEATURES=sqe-catalog/full-backends`.
ARG CATALOG_FEATURES=
COPY --from=planner /build/recipe.json recipe.json
COPY vendor/ vendor/
RUN --mount=type=cache,id=sqe-cargo-registry-${TARGETARCH},sharing=locked,target=/usr/local/cargo/registry \
    --mount=type=cache,id=sqe-cargo-git-${TARGETARCH},sharing=locked,target=/usr/local/cargo/git \
    --mount=type=cache,id=sqe-sccache-${TARGETARCH},sharing=locked,target=/sccache \
    --mount=type=cache,id=sqe-target-${CARGO_PROFILE}-${TARGETARCH},sharing=locked,target=/build/target \
    FEATURES=""; if [ -n "$CATALOG_FEATURES" ]; then FEATURES="--features ${CATALOG_FEATURES}"; fi; \
    cargo chef cook --locked --profile "${CARGO_PROFILE}" \
      --recipe-path recipe.json --no-default-features $FEATURES \
      --package sqe-server --package sqe-cli --package sqe-bench \
    && sccache --show-stats

FROM deps AS builder
ARG TARGETARCH
ARG CARGO_PROFILE=release
ARG CATALOG_FEATURES=
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
COPY vendor/ vendor/
COPY xtask/ xtask/
RUN --mount=type=cache,id=sqe-cargo-registry-${TARGETARCH},sharing=locked,target=/usr/local/cargo/registry \
    --mount=type=cache,id=sqe-cargo-git-${TARGETARCH},sharing=locked,target=/usr/local/cargo/git \
    --mount=type=cache,id=sqe-sccache-${TARGETARCH},sharing=locked,target=/sccache \
    --mount=type=cache,id=sqe-target-${CARGO_PROFILE}-${TARGETARCH},sharing=locked,target=/build/target \
    FEATURES=""; if [ -n "$CATALOG_FEATURES" ]; then FEATURES="--features ${CATALOG_FEATURES}"; fi; \
    cargo build --locked --profile "${CARGO_PROFILE}" --no-default-features $FEATURES \
      --bin sqe-server --bin sqe-cli --bin sqe-bench \
    && mkdir -p /build/out \
    && cp "target/${CARGO_PROFILE}/sqe-server" \
      "target/${CARGO_PROFILE}/sqe-cli" \
      "target/${CARGO_PROFILE}/sqe-bench" /build/out/ \
    && sccache --show-stats

# Digest-pinned; Renovate bumps the runtime independently of builder tooling.
# Keep this digest in lockstep with Dockerfile.bench.
FROM cgr.dev/chainguard/glibc-dynamic@sha256:82edc253a57efee78d0fb504e11a93b7c74687b1b736110ad3a2a4f3edf632ab AS runtime-base

USER 65532

FROM busybox:1.38.0-uclibc@sha256:b699b0c739b2d039afd8e2488a3ccfc0b9a86301d87633448d4a93c57bee8c76 AS healthcheck-bin

FROM runtime-base AS bench-runtime

LABEL org.opencontainers.image.title="sqe-bench" \
      org.opencontainers.image.description="TPC benchmark data generator for SQE"

COPY --from=builder /build/out/sqe-bench /usr/local/bin/

ENTRYPOINT ["/usr/local/bin/sqe-bench"]

# Keep runtime last so plain `docker build .` remains the server image.
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

COPY --from=builder /build/out/sqe-server /build/out/sqe-cli /usr/local/bin/
COPY --from=healthcheck-bin /bin/wget /usr/local/bin/wget

EXPOSE 50051 50052 8080 9090 9091

HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
    CMD ["/usr/local/bin/wget", "-q", "-O", "/dev/null", "http://127.0.0.1:9091/healthz"]

ENTRYPOINT ["/usr/local/bin/sqe-server"]
