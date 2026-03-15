# ── Build stage ──────────────────────────────────────────────
FROM rust:1.85-alpine AS builder

RUN apk add --no-cache musl-dev cmake protobuf-dev openssl-dev openssl-libs-static pkgconfig

WORKDIR /build

# Guarantee static linking with musl
ENV RUSTFLAGS="-C target-feature=+crt-static" \
    OPENSSL_STATIC=1 \
    OPENSSL_NO_VENDOR=1

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
    mkdir -p "$dir/src"; \
    echo "fn main() {}" > "$dir/src/main.rs"; \
    echo "" > "$dir/src/lib.rs"; \
    ' _ {} \;
RUN cargo build --release --bin sqe-coordinator --bin sqe-worker --bin sqe-cli 2>/dev/null || true

# ── Full build ──────────────────────────────────────────────
# Copy real sources — only recompiles workspace crates, not deps
COPY crates/ crates/
RUN touch crates/*/src/*.rs && \
    cargo build --release --bin sqe-coordinator --bin sqe-worker --bin sqe-cli

# ── Coordinator image ────────────────────────────────────────
FROM alpine:3.21 AS coordinator

RUN apk add --no-cache ca-certificates && \
    addgroup -S sqe && adduser -S sqe -G sqe

COPY --from=builder /build/target/release/sqe-coordinator /usr/local/bin/

USER sqe
EXPOSE 50051 8080 9090
ENTRYPOINT ["sqe-coordinator"]

# ── Worker image ─────────────────────────────────────────────
FROM alpine:3.21 AS worker

RUN apk add --no-cache ca-certificates && \
    addgroup -S sqe && adduser -S sqe -G sqe

COPY --from=builder /build/target/release/sqe-worker /usr/local/bin/

USER sqe
EXPOSE 50052
ENTRYPOINT ["sqe-worker"]

# ── CLI image (lightweight) ──────────────────────────────────
FROM alpine:3.21 AS cli

RUN apk add --no-cache ca-certificates && \
    addgroup -S sqe && adduser -S sqe -G sqe

COPY --from=builder /build/target/release/sqe-cli /usr/local/bin/

USER sqe
ENTRYPOINT ["sqe-cli"]
