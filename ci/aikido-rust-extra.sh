#!/usr/bin/env bash
# Repo-specific Rust checks: the sqe-catalog feature matrix, the Iceberg
# matrix score floor and the workspace-hack drift check. Run by the platform's
# `aikido-rust-extra` job (aikido >= v0.12.0), in parallel with the test gate.
#
set -e

# SQE_CI_PROFILE (set in .gitlab-ci.yml `workflow:`): MRs default to
# "minimal", main and tags to "full". Minimal keeps the cheap xtask guards and
# skips what compiles the workspace a second time next to the test gate:
# clippy (a check-mode rebuild of every crate), the sqe-catalog feature
# matrix, and the from-source cargo-hakari install.
profile="${SQE_CI_PROFILE:-full}"
echo "== SQE_CI_PROFILE=$profile =="
if [ "$profile" = "minimal" ]; then
  echo "minimal: skipping clippy, the sqe-catalog feature matrix and the hakari drift check"
  echo "(label the MR ci::full and run a new pipeline to get them)"
  cargo xtask matrix-report --min-percent 50
  cargo xtask test-groups --check
  exit 0
fi

# Clippy first, advisory (it was the platform's aikido-clippy-rust job; see
# .gitlab-ci.yml). The feature-matrix checks below then reuse its
# check-mode dependency artifacts instead of compiling the tree again.
echo "== cargo clippy (advisory, max 20m) =="
rustup component add clippy >/dev/null 2>&1 || true
timeout --foreground --signal=TERM --kill-after=30s 20m \
  cargo clippy --workspace --all-targets --all-features -- -D warnings \
  || echo "clippy: findings (non-blocking)"

cargo check -p sqe-catalog --no-default-features --features rest
cargo check -p sqe-catalog --no-default-features --features rest,glue,s3tables
cargo check -p sqe-catalog --no-default-features --features rest,hms
cargo check -p sqe-catalog --no-default-features --features rest,sql-postgres
cargo check -p sqe-catalog --no-default-features --features rest,sql-sqlite
cargo check -p sqe-catalog --no-default-features --features rest,sql-sqlite-rs
cargo check -p sqe-catalog --no-default-features --features rest,sql-sqlite-rs,sql-postgres
cargo check -p sqe-catalog --no-default-features
cargo xtask matrix-report --min-percent 50

# Every #[ignore] must name the stack it needs (finding 807): a bare #[ignore]
# or a reason outside the vocabulary in xtask/src/test_groups.rs fails here.
cargo xtask test-groups --check

# The workspace-hack crate must match the dependency graph, or per-crate builds
# silently fall back to their own feature resolution and the 38-copies problem
# returns without anyone noticing. `generate --diff` exits non-zero on drift.
# cargo-hakari is not in the rust image; a from-source install is a few minutes
# and runs in parallel with the gate, so it is acceptable here for now.
if ! command -v cargo-hakari >/dev/null 2>&1; then
  cargo install cargo-hakari --locked --version 0.9.38 >/dev/null 2>&1
fi
cargo hakari generate --diff
