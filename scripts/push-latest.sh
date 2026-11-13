#!/usr/bin/env bash
# Build and push a multi-arch (linux/amd64 + linux/arm64) `:latest` of sqe.
#
# Usage:
#   make latest                     (or scripts/push-latest.sh)
#   DRY_RUN=1 make latest           builds both binaries and images, pushes nothing
#   ALLOW_BRANCH=1 make latest      push a commit that is not on origin/main
#
# What it does:
#   1. Cross-compiles sqe-server and sqe-cli for x86_64 and aarch64 Linux with
#      cargo-zigbuild, on this host, with a glibc 2.28 floor. No QEMU: the
#      compiler runs natively and zig links for the target.
#   2. Packages each architecture with Dockerfile.ci (COPY-only, the same runtime
#      stages as ./Dockerfile) and pushes it by digest.
#   3. Joins both digests into one manifest list and tags it `:latest` and
#      `:<short sha>` in every repository of LATEST_REPOS.
#   4. Fails unless every pushed tag lists both linux/amd64 and linux/arm64.
#
# Why: the release images from CI are amd64 only, so Apple silicon and arm64
# nodes either emulate or build from source. Release tags no longer write
# `:latest` (sqe-publish-latest in .gitlab-ci.yml); this script owns it until CI
# builds both architectures.
#
# Needs: cargo-zigbuild (`cargo install cargo-zigbuild`), zig on PATH (`brew
# install zig` or `pip install ziglang`), the two rustup targets, docker buildx,
# and `make login` for the registry.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

PUSH_REGISTRY="${PUSH_REGISTRY:-repo.sovereign-data.org/chameleon}"
# Both paths are in use: `make push` writes chameleon/sqe, the CI release mirror
# and ai-portal-data use chameleon/sqlengine/sqe.
LATEST_REPOS="${LATEST_REPOS:-$PUSH_REGISTRY/sqlengine/sqe $PUSH_REGISTRY/sqe}"
LATEST_TAG="${LATEST_TAG:-latest}"
GLIBC_FLOOR="${GLIBC_FLOOR:-2.28}"
CARGO_PROFILE="${CARGO_PROFILE:-release}"
# Empty = the slim REST image, as the release image ships (./Dockerfile's default).
CATALOG_FEATURES="${CATALOG_FEATURES:-}"
BUILDX_BUILDER="${BUILDX_BUILDER:-sqe-multiarch}"
DOCKER="${DOCKER:-docker}"
DRY_RUN="${DRY_RUN:-0}"
ALLOW_BRANCH="${ALLOW_BRANCH:-0}"
STAGE="$REPO_ROOT/dist/latest"

die() { echo "error: $*" >&2; exit 1; }

# ── Preflight ──────────────────────────────────────────────────────────────
command -v cargo-zigbuild >/dev/null || die "cargo-zigbuild not found (cargo install cargo-zigbuild)"
command -v zig >/dev/null || python3 -c "import ziglang" 2>/dev/null || die "zig not found (brew install zig, or pip install ziglang)"
"$DOCKER" buildx version >/dev/null || die "docker buildx not available"
for t in x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu; do
    rustup target list --installed | grep -qx "$t" || die "rust target $t missing (rustup target add $t)"
done

# The image is labelled with HEAD, so HEAD must be what gets compiled.
[[ -z "$(git status --porcelain --untracked-files=no)" ]] \
    || die "tracked files are modified; :latest must be built from a commit"
GIT_REVISION="$(git rev-parse HEAD)"
SHORT_SHA="$(git rev-parse --short=9 HEAD)"
if [[ "$ALLOW_BRANCH" != 1 ]]; then
    git fetch -q origin main
    git merge-base --is-ancestor HEAD origin/main \
        || die "HEAD $SHORT_SHA is not on origin/main (ALLOW_BRANCH=1 to push it anyway)"
fi
VERSION="$(sed -n '/^\[workspace\.package\]/,/^\[/s/^version = "\(.*\)"/\1/p' Cargo.toml)"
# The commit date, not "now": the same commit then gives the same image.
SOURCE_DATE_EPOCH="$(git log -1 --format=%ct HEAD)"
export SOURCE_DATE_EPOCH
BUILD_DATE="$(TZ=UTC git log -1 --format=%cd --date=format-local:%Y-%m-%dT%H:%M:%SZ HEAD)"

# One word, or none (macOS /bin/bash 3.2 rejects an empty array under set -u).
FEATURE_ARG=""
[[ -n "$CATALOG_FEATURES" ]] && FEATURE_ARG="--features=$CATALOG_FEATURES"

# ── 1. Cross-compile ───────────────────────────────────────────────────────
# .cargo/config.toml links x86_64 Linux with clang + mold for native builds;
# zig is the linker here, so drop them. A set RUSTFLAGS (even empty) wins over
# every target.<triple>.rustflags (an empty CARGO_TARGET_*_RUSTFLAGS does not)
# and, with --target, does not reach the host's build scripts.
#
# The remaps keep this machine's paths (home directory, user name) out of the
# panic messages and debug strings of a published binary. Later flags win, so
# $HOME is the catch-all and the specific prefixes follow it.
CARGO_HOME_DIR="${CARGO_HOME:-$HOME/.cargo}"
RUSTUP_HOME_DIR="${RUSTUP_HOME:-$HOME/.rustup}"
case "$HOME$CARGO_HOME_DIR$RUSTUP_HOME_DIR$REPO_ROOT" in
    *[[:space:]]*) die "a space in HOME, CARGO_HOME, RUSTUP_HOME or the checkout path breaks RUSTFLAGS" ;;
esac
REMAP="--remap-path-prefix=$HOME=/home --remap-path-prefix=$CARGO_HOME_DIR=/cargo"
REMAP="$REMAP --remap-path-prefix=$RUSTUP_HOME_DIR=/rustup --remap-path-prefix=$REPO_ROOT=/build"
# C sources of build scripts (aws-lc, zstd, sqlite) embed __FILE__ the same way.
CFLAG_REMAP="-ffile-prefix-map=$HOME=/home -ffile-prefix-map=$CARGO_HOME_DIR=/cargo -ffile-prefix-map=$REPO_ROOT=/build"
for pair in amd64:x86_64-unknown-linux-gnu arm64:aarch64-unknown-linux-gnu; do
    arch="${pair%%:*}"; triple="${pair#*:}"
    echo "==> cargo zigbuild $triple (glibc $GLIBC_FLOOR, profile $CARGO_PROFILE)"
    RUSTFLAGS="$REMAP" CFLAGS="$CFLAG_REMAP" CXXFLAGS="$CFLAG_REMAP" \
        cargo zigbuild --locked --profile "$CARGO_PROFILE" --no-default-features ${FEATURE_ARG:+"$FEATURE_ARG"} \
        --target "$triple.$GLIBC_FLOOR" -p sqe-server -p sqe-cli --bin sqe-server --bin sqe-cli
    profile_dir="$CARGO_PROFILE"; [[ "$CARGO_PROFILE" == dev ]] && profile_dir=debug
    rm -rf "${STAGE:?}/$arch"
    mkdir -p "$STAGE/$arch/out"
    cp "target/$triple/$profile_dir/sqe-server" "target/$triple/$profile_dir/sqe-cli" "$STAGE/$arch/out/"
    cp Dockerfile.ci "$STAGE/$arch/Dockerfile"
    if grep -a -q -F -e "$HOME" "$STAGE/$arch/out/sqe-server" "$STAGE/$arch/out/sqe-cli"; then
        die "$arch binaries still embed $HOME; a path remap is missing"
    fi
done

# ── 2. One image per architecture, pushed by digest ────────────────────────
"$DOCKER" buildx inspect "$BUILDX_BUILDER" >/dev/null 2>&1 \
    || "$DOCKER" buildx create --name "$BUILDX_BUILDER" --driver docker-container --bootstrap >/dev/null

PRIMARY="${LATEST_REPOS%% *}"
DIGEST_amd64=""; DIGEST_arm64=""
for arch in amd64 arm64; do
    echo "==> image linux/$arch"
    meta="$STAGE/$arch/metadata.json"
    if [[ "$DRY_RUN" == 1 ]]; then out="type=image,name=$PRIMARY,rewrite-timestamp=true,push=false"
    else out="type=image,name=$PRIMARY,rewrite-timestamp=true,push-by-digest=true,name-canonical=true,push=true"
    fi
    # No provenance or SBOM attestation: both carry timestamps, so the digest
    # would differ on every run, and each adds an unknown/unknown entry to the
    # manifest list.
    "$DOCKER" buildx build --builder "$BUILDX_BUILDER" --platform "linux/$arch" \
        --provenance=false --sbom=false \
        --build-arg VERSION="$VERSION" --build-arg BUILD_DATE="$BUILD_DATE" \
        --build-arg GIT_REVISION="$GIT_REVISION" \
        --output "$out" --metadata-file "$meta" \
        -f "$STAGE/$arch/Dockerfile" "$STAGE/$arch"
    digest="$(python3 -I -c 'import json,sys; print(json.load(open(sys.argv[1]))["containerimage.digest"])' "$meta")"
    if [[ "$arch" == amd64 ]]; then DIGEST_amd64="$digest"; else DIGEST_arm64="$digest"; fi
    echo "    linux/$arch $digest"
done

if [[ "$DRY_RUN" == 1 ]]; then
    echo ""
    echo "DRY_RUN: built both images, pushed nothing. Would tag :$LATEST_TAG and :$SHORT_SHA in: $LATEST_REPOS"
    exit 0
fi

# ── 3. Manifest list, in every repository ──────────────────────────────────
for repo in $LATEST_REPOS; do
    echo "==> $repo:$LATEST_TAG + :$SHORT_SHA"
    "$DOCKER" buildx imagetools create -t "$repo:$LATEST_TAG" -t "$repo:$SHORT_SHA" \
        "$PRIMARY@$DIGEST_amd64" "$PRIMARY@$DIGEST_arm64"
done

# ── 4. Verify ──────────────────────────────────────────────────────────────
status=0
for repo in $LATEST_REPOS; do
    for tag in "$LATEST_TAG" "$SHORT_SHA"; do
        platforms="$("$DOCKER" buildx imagetools inspect --format '{{json .Manifest}}' "$repo:$tag" \
            | python3 -I -c 'import json,sys; print(",".join(sorted(m["platform"]["os"]+"/"+m["platform"]["architecture"] for m in json.load(sys.stdin).get("manifests",[]) if m.get("platform",{}).get("os")!="unknown")))')"
        if [[ ",$platforms," == *,linux/amd64,* && ",$platforms," == *,linux/arm64,* ]]; then
            echo "  ok  $repo:$tag [$platforms]"
        else
            echo "  !!  $repo:$tag [$platforms]: expected linux/amd64 and linux/arm64" >&2
            status=1
        fi
    done
done
[[ $status == 0 ]] || exit 1
echo ""
echo "Pushed sqe $VERSION ($SHORT_SHA) as :$LATEST_TAG for linux/amd64 + linux/arm64."
