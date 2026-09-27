#!/usr/bin/env bash
# Build the third-party dependency tree once and publish it as one extra layer
# on top of the platform Rust image: $CI_REGISTRY_IMAGE/ci-deps:main. The Rust
# jobs then start from that image with CARGO_TARGET_DIR pointing at the baked
# tree, so cargo only compiles what differs from main (normally: our crates).
#
# Why it is safe to use a stale image: freshness is still cargo's call. A
# registry crate is reused only if its package id, features, profile and
# rustc all match; anything else is rebuilt. Our own crates are always
# rebuilt (a fresh checkout makes their sources newer than the baked
# artifacts); vendored crates are reused only when vendor/ is unchanged (see
# the mtime pin below and ci/reuse-deps-image.sh). Measured locally
# (2026-09-27), fresh clone reusing a baked target: 269 s cold -> 118 s, and
# 101 s with the vendor/ mtimes pinned.
#
# Paths must match the consumers exactly: the same checkout path (runner
# builds dir), CARGO_HOME, CARGO_TARGET_DIR and debuginfo env. The job runs
# the same `.aikido_rust_build_env` before_script as the gate.
set -euo pipefail

: "${SQE_DEPS_TARGET:?}" "${CI_REGISTRY_IMAGE:?}" "${AIKIDO_RUST_IMAGE:?}"
export CARGO_TARGET_DIR="$SQE_DEPS_TARGET"
# Created up front: the vendor-tree marker below is written before cargo runs.
mkdir -p "$CARGO_TARGET_DIR"

# Pinned like sccache in the platform template: version + sha256 of the
# release tarball (go-containerregistry checksums.txt).
CRANE_VERSION="v0.22.1"
CRANE_SHA256="0ab7a1d6932a213aed964ce97666c3077fe691c8606413674a8b3e0b9ec4cda0"
tmp="$(mktemp -d)"
curl -fsSL -o "$tmp/crane.tgz" \
  "https://github.com/google/go-containerregistry/releases/download/${CRANE_VERSION}/go-containerregistry_Linux_x86_64.tar.gz"
echo "${CRANE_SHA256}  $tmp/crane.tgz" | sha256sum -c -
tar -xzf "$tmp/crane.tgz" -C "$tmp" crane

# Vendored crates are path dependencies, so cargo judges them by mtime and a
# fresh checkout would rebuild them. Pin every vendor/ file to one fixed old
# date and record the vendor/ tree hash next to the target. A consumer resets
# its mtimes to the same date ONLY when its vendor/ tree hash matches
# (ci/reuse-deps-image.sh), so a changed vendored crate is never mistaken for
# the baked one.
# `-t` rather than `-d @epoch` so the same line works with BSD touch locally.
find vendor -exec touch -h -t 200001010000 {} +
git rev-parse HEAD:vendor > "$CARGO_TARGET_DIR.vendor-tree"

# What the gate compiles (`cargo test --no-run`) and what aikido-rust-extra
# compiles first (clippy, check mode). Same flags as those jobs.
cargo test --workspace --all-features --locked --no-run
rustup component add clippy >/dev/null 2>&1 || true
cargo clippy --workspace --all-targets --all-features --locked

# Keep only what a later build can reuse: third-party libraries, build-script
# output and fingerprints. Test binaries and our own crates are rebuilt by
# every consumer anyway, and they are most of the 12 GB.
debug="$CARGO_TARGET_DIR/debug"
find "$debug" -maxdepth 1 -type f -delete
rm -rf "$debug/examples" "$debug/incremental"
find "$debug/deps" -type f -perm -u+x ! -name '*.so' -delete
find "$debug/deps" -type f \( -name '*sqe_*' -o -name '*xtask*' -o -name '*workspace_hack*' \) -delete
find "$debug/.fingerprint" "$debug/build" -mindepth 1 -maxdepth 1 \
  \( -name 'sqe-*' -o -name 'xtask-*' -o -name 'workspace-hack-*' \) -exec rm -rf {} +
du -sh "$CARGO_TARGET_DIR"

lock="$(sha256sum Cargo.lock | cut -c1-12)"
tar -C / -czf "$tmp/deps.tgz" "${CARGO_TARGET_DIR#/}" "${CARGO_TARGET_DIR#/}.vendor-tree"
ls -la "$tmp/deps.tgz"

"$tmp/crane" auth login "$CI_REGISTRY" -u "$CI_REGISTRY_USER" -p "$CI_REGISTRY_PASSWORD"
# Default branch: `lock-<hash>` (a record of what each Cargo.lock built) plus
# the moving `main` the Rust jobs pull. An MR trial run sets
# SQE_DEPS_TAG=mr-<iid> and writes that tag only.
if [ -n "${SQE_DEPS_TAG:-}" ]; then
  "$tmp/crane" append --base "$AIKIDO_RUST_IMAGE" --new_layer "$tmp/deps.tgz" \
    --new_tag "$CI_REGISTRY_IMAGE/ci-deps:$SQE_DEPS_TAG"
  echo "published $CI_REGISTRY_IMAGE/ci-deps:$SQE_DEPS_TAG ($(git rev-parse --short HEAD))"
else
  "$tmp/crane" append --base "$AIKIDO_RUST_IMAGE" --new_layer "$tmp/deps.tgz" \
    --new_tag "$CI_REGISTRY_IMAGE/ci-deps:lock-$lock"
  "$tmp/crane" tag "$CI_REGISTRY_IMAGE/ci-deps:lock-$lock" main
  echo "published $CI_REGISTRY_IMAGE/ci-deps:main (= lock-$lock, $(git rev-parse --short HEAD))"
fi
