#!/usr/bin/env bash
# Fast inner loop for the server binary: parallel rustc frontend + cranelift
# code generation. Opt-in; `cargo dev` / `make dev` (stable, LLVM) stays the
# reference build and the one CI reproduces.
#
# Measured 2026-10-19 (M3 Pro, 36 GB, DataFusion 55.2), warm cache:
#   stable `cargo dev-server`                1 m 18 s  (sqe-core edit)
#   this script                              0 m 36 s  (sqe-core edit)
#                                            0 m 20 s  (sqe-coordinator edit)
#   cold build                               2 m 41 s (vs 4 m 11 s stable)
#
# Artifacts live in target/fast so toggling this on and off does not invalidate
# the stable cache in target/debug (RUSTFLAGS are part of the fingerprint).
#
# Usage:
#   scripts/dev-fast.sh                       # build sqe-server
#   scripts/dev-fast.sh check --bin sqe-server
#   scripts/dev-fast.sh test -p sqe-core --lib variant
#   FAST_THREADS=6 scripts/dev-fast.sh        # fewer frontend threads
#
# Needs the nightly toolchain (`-Zthreads` and `-Zcodegen-backend` are
# unstable): rustup toolchain install nightly
#
# Nightly also prints one `recursion_depth_exceeding_limit` future-incompat
# warning on sqe-server that stable does not; it is a lint, not a build error.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

THREADS="${FAST_THREADS:-8}"
TARGET_DIR="${FAST_TARGET_DIR:-$ROOT_DIR/target/fast}"

if ! rustup toolchain list 2>/dev/null | grep -q '^nightly'; then
  cat >&2 <<'EOF'
scripts/dev-fast.sh needs the nightly toolchain: -Zthreads and
-Zcodegen-backend are unstable. Install it with:

    rustup toolchain install nightly

Stable `cargo dev` / `cargo check-server` / `make dev` are unaffected.
EOF
  exit 1
fi

# RUSTFLAGS replaces the rustflags from .cargo/config.toml, so the linker and
# target-cpu flags configured there have to be repeated here.
case "$(uname -s)" in
  Darwin)
    RUSTFLAGS="-Zthreads=$THREADS -C target-cpu=native -C link-arg=-fuse-ld=/opt/homebrew/bin/ld64.lld"
    ;;
  Linux)
    RUSTFLAGS="-Zthreads=$THREADS -C target-cpu=native -C linker=clang -C link-arg=-fuse-ld=mold"
    ;;
  *)
    RUSTFLAGS="-Zthreads=$THREADS"
    ;;
esac

if [ "$#" -eq 0 ]; then
  set -- build --no-default-features --bin sqe-server
fi

exec env \
  CARGO_TARGET_DIR="$TARGET_DIR" \
  CARGO_INCREMENTAL=0 \
  RUSTFLAGS="$RUSTFLAGS" \
  cargo +nightly -Zcodegen-backend --config .cargo/fast.toml "$@"
