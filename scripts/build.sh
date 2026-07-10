#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$ROOT_DIR"

PROFILE="${1:-release}"

case "$PROFILE" in
    release)
        echo "Building release binaries..."
        cargo build --release --no-default-features --bin sqe-server --bin sqe-cli
        BIN_DIR="target/release"
        ;;
    debug)
        echo "Building debug binaries..."
        cargo build --no-default-features --bin sqe-server --bin sqe-cli
        BIN_DIR="target/debug"
        ;;
    *)
        echo "Usage: $0 [release|debug]"
        exit 1
        ;;
esac

echo ""
echo "Binaries:"
ls -lh "$BIN_DIR/sqe-server" "$BIN_DIR/sqe-cli"
