#!/usr/bin/env bash
# Repo-specific Rust checks run inside aikido test-rust (C toolchain installed there).
set -e
cargo check -p sqe-catalog --no-default-features --features rest
cargo check -p sqe-catalog --no-default-features --features rest,glue,s3tables
cargo check -p sqe-catalog --no-default-features --features rest,hms
cargo check -p sqe-catalog --no-default-features --features rest,sql-postgres
cargo check -p sqe-catalog --no-default-features --features rest,sql-sqlite
cargo check -p sqe-catalog --no-default-features
cargo xtask matrix-report --min-percent 50
