#!/usr/bin/env bash
set -euo pipefail

# Full build + test pipeline: unit tests, then integration tests.
# Usage: ./scripts/build-test.sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$ROOT_DIR"

echo "=== Step 1: Build ==="
cargo build --release --no-default-features --bin sqe-server --bin sqe-cli
echo ""

echo "=== Step 2: Unit tests ==="
cargo test --workspace --exclude sqe-cli
cargo test --package sqe-cli --no-default-features
echo ""

echo "=== Step 3: Integration tests ==="
"$SCRIPT_DIR/integration-test.sh"
