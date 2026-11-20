# syntax=docker/dockerfile:1
#
# Shared fast build for local Compose, data-platform quickstart/sqe, and CI.
# Dependencies are isolated with cargo-chef and compiler/target state survives
# source edits in locked BuildKit caches. Runtime and bench-runtime consume the
# same target cache, so building both targets compiles the Rust graph only once.
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
FROM lukemathwalker/cargo-chef:latest-rust-bookworm@sha256:9573268a2a874efc1c755a0094a50c25988b2c6a0b7a520ddb7fd08d868d1c43 AS cargo-chef-bin

# Renovate's "container images" group bumps tag and digest together; the rust tag must match rust-toolchain.toml.
FROM rust:1.99.1-bookworm@sha256:7085845219c18e7c5b1f34ed9ce89b84f822f2e3d54a02774cedc996ac0f304f AS chef

COPY --from=cargo-chef-bin /usr/local/cargo/bin/cargo-chef /usr/local/cargo/bin/cargo-chef

# libprotobuf-dev supplies google/protobuf/*.proto for datafusion-substrait.
# lld materially shortens the final coordinator link. aws-lc-sys builds with
# cc alone, so cmake is not installed (a cold build of all 603 crates passes
# without it). The base image already ships libssl-dev, pkg-config and curl.
#
# No sccache: the target cache mounts below already keep every compiled crate,
# and the workspace crates that do recompile after an edit never hit it
# (measured 0 hits of 15, final build step 495 s with it and 481 s without).
RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler libprotobuf-dev clang lld \
    && rm -rf /var/lib/apt/lists/*

ENV RUSTFLAGS="-C linker=clang -C link-arg=-fuse-ld=lld"

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
    --mount=type=cache,id=sqe-target-${CARGO_PROFILE}-${TARGETARCH},sharing=locked,target=/build/target \
    FEATURES=""; if [ -n "$CATALOG_FEATURES" ]; then FEATURES="--features ${CATALOG_FEATURES}"; fi; \
    cargo chef cook --locked --profile "${CARGO_PROFILE}" \
      --recipe-path recipe.json --no-default-features $FEATURES \
      --package sqe-server --package sqe-cli --package sqe-bench

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
    --mount=type=cache,id=sqe-target-${CARGO_PROFILE}-${TARGETARCH},sharing=locked,target=/build/target \
    FEATURES=""; if [ -n "$CATALOG_FEATURES" ]; then FEATURES="--features ${CATALOG_FEATURES}"; fi; \
    # cargo writes the `dev` profile to target/debug.
    PROFILE_DIR="${CARGO_PROFILE}"; if [ "$CARGO_PROFILE" = dev ]; then PROFILE_DIR=debug; fi; \
    cargo build --locked --profile "${CARGO_PROFILE}" --no-default-features $FEATURES \
      --bin sqe-server --bin sqe-cli \
    && mkdir -p /build/out \
    && cp "target/${PROFILE_DIR}/sqe-server" "target/${PROFILE_DIR}/sqe-cli" /build/out/

# sqe-bench links only when a build asks for `bench-runtime`. It shares the
# target cache with `builder`, so its dependencies are already compiled.
FROM builder AS bench-builder
ARG TARGETARCH
ARG CARGO_PROFILE=release
ARG CATALOG_FEATURES=
RUN --mount=type=cache,id=sqe-cargo-registry-${TARGETARCH},sharing=locked,target=/usr/local/cargo/registry \
    --mount=type=cache,id=sqe-cargo-git-${TARGETARCH},sharing=locked,target=/usr/local/cargo/git \
    --mount=type=cache,id=sqe-target-${CARGO_PROFILE}-${TARGETARCH},sharing=locked,target=/build/target \
    FEATURES=""; if [ -n "$CATALOG_FEATURES" ]; then FEATURES="--features ${CATALOG_FEATURES}"; fi; \
    # cargo writes the `dev` profile to target/debug.
    PROFILE_DIR="${CARGO_PROFILE}"; if [ "$CARGO_PROFILE" = dev ]; then PROFILE_DIR=debug; fi; \
    cargo build --locked --profile "${CARGO_PROFILE}" --no-default-features $FEATURES \
      --bin sqe-bench \
    && cp "target/${PROFILE_DIR}/sqe-bench" /build/out/

# Digest-pinned; Renovate bumps the runtime independently of builder tooling.
# Keep this digest in lockstep with Dockerfile.ci.
FROM cgr.dev/chainguard/glibc-dynamic@sha256:3359c6ba6b3bcf3b1d2179849f782365564cb262789e732e2833b25b9a8c00f1 AS runtime-base

USER 65532

FROM busybox:1.38.0-uclibc@sha256:b2e35951f72501f6a99f1bf45bcfe6d4795be08c518c2f97d0ed2fe776958a3b AS healthcheck-bin

FROM runtime-base AS bench-runtime

LABEL org.opencontainers.image.title="sqe-bench" \
      org.opencontainers.image.description="TPC benchmark data generator for SQE"

COPY --from=bench-builder /build/out/sqe-bench /usr/local/bin/

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
