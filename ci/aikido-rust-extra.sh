#!/usr/bin/env bash
# Repo-specific Rust checks: the sqe-catalog feature matrix and the Iceberg
# matrix score floor. Run by the `rust-feature-matrix` job in .gitlab-ci.yml,
# in parallel with the test gate.
#
# The aikido test-rust template still calls this script at the end of its own
# job (`./ci/aikido-rust-extra.sh || echo ...`). Measured 2026-09-04: 7m49s
# serial behind the tests, advisory, so it neither gated nor finished early.
# Detect that caller and step aside; the dedicated job is the one that counts.
set -e
if [ "${CI_JOB_NAME:-}" = "aikido-test-rust" ]; then
  echo "feature-matrix checks run in the rust-feature-matrix job; skipping here"
  exit 0
fi
cargo check -p sqe-catalog --no-default-features --features rest
cargo check -p sqe-catalog --no-default-features --features rest,glue,s3tables
cargo check -p sqe-catalog --no-default-features --features rest,hms
cargo check -p sqe-catalog --no-default-features --features rest,sql-postgres
cargo check -p sqe-catalog --no-default-features --features rest,sql-sqlite
cargo check -p sqe-catalog --no-default-features
cargo xtask matrix-report --min-percent 50

# The workspace-hack crate must match the dependency graph, or per-crate builds
# silently fall back to their own feature resolution and the 38-copies problem
# returns without anyone noticing. `generate --diff` exits non-zero on drift.
# cargo-hakari is not in the rust image; a from-source install is a few minutes
# and runs in parallel with the gate, so it is acceptable here for now.
if ! command -v cargo-hakari >/dev/null 2>&1; then
  cargo install cargo-hakari --locked --version 0.9.38 >/dev/null 2>&1
fi
cargo hakari generate --diff
