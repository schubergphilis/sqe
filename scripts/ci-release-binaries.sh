#!/usr/bin/env bash
# Cross-compile sqe-server and sqe-cli for one Linux architecture, in CI.
#
# Usage: scripts/ci-release-binaries.sh <amd64|arm64>
#
# The release tag pipeline runs this once per architecture (sqe-release-binaries
# in .gitlab-ci.yml) on the amd64 runner. cargo-zigbuild links for the target
# with zig, with a glibc 2.28 floor, so no arm64 runner and no QEMU are needed.
# Same build as step 1 of scripts/push-latest.sh, which builds `:latest` from a
# workstation; keep the two cargo lines alike.
#
# Output: dist/<arch>/out/sqe-server and sqe-cli, the layout Dockerfile.ci
# copies from, so dist/<arch> is the kaniko build context.
#
# Env: CARGO_PROFILE (release), GLIBC_FLOOR (2.28).

set -euo pipefail

ARCH="${1:?usage: $0 <amd64|arm64>}"
case "$ARCH" in
    amd64) TRIPLE=x86_64-unknown-linux-gnu ;;
    arm64) TRIPLE=aarch64-unknown-linux-gnu ;;
    *) echo "error: unknown arch '$ARCH' (amd64 or arm64)" >&2; exit 1 ;;
esac
CARGO_PROFILE="${CARGO_PROFILE:-release}"
GLIBC_FLOOR="${GLIBC_FLOOR:-2.28}"

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

# Pinned like the tools in the aikido-rust image: a version and the sha256 of
# its x86_64 Linux release tarball move together, by hand. The runner fleet is
# amd64, so only that build of each tool is pinned.
ZIG_VERSION="0.14.1"
ZIG_SHA256="24aeeec8af16c381934a6cd7d95c807a8cb2cf7df9fa40d359aa884195c4716c"
ZIGBUILD_VERSION="0.24.1"
ZIGBUILD_SHA256="f09b180a517af0e4dd4bffc921bb88e5ab705452a9dd948c61e489b3020c68c2"

TOOLS="${SQE_ZIG_TOOLS_DIR:-$REPO_ROOT/.zig-tools}"
if [[ "$(uname -m)" != x86_64 ]]; then
    # nosemgrep: opt.aikido.semgrep-rules.ai.ai-best-practices.hooks-dns-exfiltration.hooks-dns-exfiltration-generic -- prints the host architecture to stderr, no network
    echo "error: the pinned zig and cargo-zigbuild are x86_64 Linux builds; this host is $(uname -m)" >&2
    exit 1
fi

# Both tarballs are .tar.xz; the slim Rust image has no xz.
if ! command -v xz >/dev/null; then
    apt-get update -qq
    apt-get install -y -qq --no-install-recommends xz-utils >/dev/null
fi

# fetch <url> <sha256> <dest-dir>: download, verify, then extract.
fetch() {
    local tmp
    tmp="$(mktemp -d)"
    curl -fsSL -o "$tmp/pkg.tar.xz" "$1"
    echo "$2  $tmp/pkg.tar.xz" | sha256sum -c - >/dev/null \
        || { echo "error: sha256 mismatch for $1" >&2; exit 1; }
    mkdir -p "$3"
    tar -xJf "$tmp/pkg.tar.xz" -C "$3" --strip-components=1
    rm -rf "$tmp"
}

if [[ ! -x "$TOOLS/zig/zig" ]]; then
    fetch "https://ziglang.org/download/$ZIG_VERSION/zig-x86_64-linux-$ZIG_VERSION.tar.xz" \
        "$ZIG_SHA256" "$TOOLS/zig"
fi
if [[ ! -x "$TOOLS/zigbuild/cargo-zigbuild" ]]; then
    fetch "https://github.com/rust-cross/cargo-zigbuild/releases/download/v$ZIGBUILD_VERSION/cargo-zigbuild-x86_64-unknown-linux-musl.tar.xz" \
        "$ZIGBUILD_SHA256" "$TOOLS/zigbuild"
fi
export PATH="$TOOLS/zig:$TOOLS/zigbuild:$PATH"
echo "zig $(zig version), $(cargo-zigbuild --version)"

# rust-toolchain.toml pins the compiler; the target is added to that toolchain.
rustup target add "$TRIPLE"

# .cargo/config.toml links x86_64 Linux with clang + mold for native builds;
# zig is the linker here. A set RUSTFLAGS wins over every
# target.<triple>.rustflags and, with --target, does not reach build scripts.
# The remaps keep runner paths out of panic messages and debug strings.
CARGO_HOME_DIR="${CARGO_HOME:-$HOME/.cargo}"
REMAP="--remap-path-prefix=$CARGO_HOME_DIR=/cargo --remap-path-prefix=$REPO_ROOT=/build"
CFLAG_REMAP="-ffile-prefix-map=$CARGO_HOME_DIR=/cargo -ffile-prefix-map=$REPO_ROOT=/build"

# zig's lld keeps every input object open while it links: sqe-server passes
# ~1650 objects, past the runner's default soft limit of 1024 descriptors, and
# the link fails with ProcessFdQuotaExceeded (v0.55.0). Raise the soft limit to
# the hard one; a container that caps the hard limit below this fails here, not
# twenty minutes into the build.
ulimit -n "$(ulimit -Hn)"
if [[ "$(ulimit -n)" != unlimited && "$(ulimit -n)" -lt 4096 ]]; then
    echo "error: open-file limit is $(ulimit -n); zig needs about 2000 to link sqe-server" >&2
    exit 1
fi

echo "==> cargo zigbuild $TRIPLE (glibc $GLIBC_FLOOR, profile $CARGO_PROFILE)"
RUSTFLAGS="$REMAP" CFLAGS="$CFLAG_REMAP" CXXFLAGS="$CFLAG_REMAP" \
    cargo zigbuild --locked --profile "$CARGO_PROFILE" --no-default-features \
    --target "$TRIPLE.$GLIBC_FLOOR" -p sqe-server -p sqe-cli --bin sqe-server --bin sqe-cli

profile_dir="$CARGO_PROFILE"
[[ "$CARGO_PROFILE" == dev ]] && profile_dir=debug
target_dir="${CARGO_TARGET_DIR:-target}"
rm -rf "dist/$ARCH"
mkdir -p "dist/$ARCH/out"
cp "$target_dir/$TRIPLE/$profile_dir/sqe-server" "$target_dir/$TRIPLE/$profile_dir/sqe-cli" "dist/$ARCH/out/"
ls -la "dist/$ARCH/out/"
