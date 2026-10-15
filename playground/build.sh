#!/usr/bin/env bash
# Build the wasm bundle and drop it in www/pkg.
#
# Needs: rustup target add wasm32-unknown-unknown
#        cargo install wasm-bindgen-cli --version 0.2.127
# The wasm-bindgen CLI version MUST match the wasm-bindgen crate version in
# Cargo.toml, or the generated JS glue will not match the wasm exports.
set -euo pipefail

cd "$(dirname "$0")"

PROFILE="${PROFILE:-release}"
BINDGEN="${BINDGEN:-wasm-bindgen}"
command -v "$BINDGEN" >/dev/null || BINDGEN="$HOME/.cargo/bin/wasm-bindgen"

echo "==> cargo build --$PROFILE"
if [ "$PROFILE" = "release" ]; then
    cargo build --release
else
    cargo build
fi

# The repo's root .cargo/config.toml pins a single shared target-dir to keep
# parallel builds from duplicating artifacts, and that setting wins over this
# directory's config. So ask cargo where the output actually landed instead of
# assuming ./target.
TARGET_DIR=$(cargo metadata --format-version 1 --no-deps \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')
WASM="$TARGET_DIR/wasm32-unknown-unknown/$PROFILE/sqe_playground.wasm"

echo "==> wasm-bindgen"
"$BINDGEN" --target web --out-dir www/pkg "$WASM"

# Optional. Typically shaves another 15-25% off the raw bundle.
if command -v wasm-opt >/dev/null; then
    echo "==> wasm-opt -Oz"
    wasm-opt -Oz -o www/pkg/sqe_playground_bg.wasm www/pkg/sqe_playground_bg.wasm
else
    echo "==> wasm-opt not found, skipping (brew install binaryen)"
fi

SIZE=$(du -h www/pkg/sqe_playground_bg.wasm | cut -f1)
GZ=$(gzip -9 -c www/pkg/sqe_playground_bg.wasm | wc -c | awk '{printf "%.1f MB", $1/1048576}')
echo
echo "    raw  $SIZE"
echo "    gzip $GZ"
echo
echo "    python3 www/serve.py     ->  http://127.0.0.1:8799"
