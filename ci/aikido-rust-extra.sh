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
