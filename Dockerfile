# ── Build stage ──────────────────────────────────────────────
FROM rust:1.85-alpine AS builder

RUN apk add --no-cache musl-dev cmake protobuf-dev openssl-dev openssl-libs-static pkgconfig

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/

# Static linking via musl — produces fully self-contained binaries
RUN cargo build --release --bin sqe-coordinator --bin sqe-worker --bin sqe-cli

# ── Coordinator image ────────────────────────────────────────
FROM alpine:3.21 AS coordinator

RUN apk add --no-cache ca-certificates

COPY --from=builder /build/target/release/sqe-coordinator /usr/local/bin/
COPY sqe.toml.example /etc/sqe/sqe.toml

EXPOSE 50051 8080 9090
ENTRYPOINT ["sqe-coordinator"]
CMD ["/etc/sqe/sqe.toml"]

# ── Worker image ─────────────────────────────────────────────
FROM alpine:3.21 AS worker

RUN apk add --no-cache ca-certificates

COPY --from=builder /build/target/release/sqe-worker /usr/local/bin/
COPY sqe.toml.example /etc/sqe/sqe.toml

EXPOSE 50052
ENTRYPOINT ["sqe-worker"]
CMD ["/etc/sqe/sqe.toml"]

# ── CLI image (lightweight) ──────────────────────────────────
FROM alpine:3.21 AS cli

RUN apk add --no-cache ca-certificates

COPY --from=builder /build/target/release/sqe-cli /usr/local/bin/

ENTRYPOINT ["sqe-cli"]
