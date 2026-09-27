#!/usr/bin/env bash
# Let a Rust job reuse the vendored crates baked into ci-deps (see
# ci/build-deps-image.sh). Cargo judges path dependencies by mtime, and a
# fresh checkout makes every vendor/ file newer than the baked artifacts. When
# this checkout's vendor/ tree is byte-identical to the one the image was
# built from, pin the mtimes to the builder's fixed date so cargo reuses them.
# On any mismatch (or outside the image) do nothing: cargo rebuilds vendor/,
# which is slower but correct.
set -euo pipefail

marker="${CARGO_TARGET_DIR:-}.vendor-tree"
if [ -z "${CARGO_TARGET_DIR:-}" ] || [ ! -f "$marker" ]; then
  echo "deps image: no baked vendor tree; vendored crates build from source"
  exit 0
fi
want="$(cat "$marker")"
have="$(git rev-parse HEAD:vendor)"
if [ "$want" = "$have" ]; then
  # Same fixed date as ci/build-deps-image.sh (both run in UTC containers).
  find vendor -exec touch -h -t 200001010000 {} +
  echo "deps image: vendor/ matches the baked tree ($have); reusing vendored crates"
else
  echo "deps image: vendor/ changed ($want -> $have); vendored crates build from source"
fi
