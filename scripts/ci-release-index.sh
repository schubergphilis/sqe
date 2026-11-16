#!/usr/bin/env bash
# Join the per-architecture release images into one manifest list (#898).
#
# Usage: scripts/ci-release-index.sh <repo> <amd64-digest> <arm64-digest> <tag>...
#
# Creates the index from <repo>@<amd64-digest> and <repo>@<arm64-digest>, tags
# it with every <tag>, checks that each tag lists linux/amd64 and linux/arm64
# and resolves to the same index digest, and writes that digest to
# image-digest.txt (what aikido-release-image-scan and the mirror jobs read).
#
# crane, not `docker buildx imagetools`: the runners have no Docker daemon.
# Taken from PATH when present, otherwise the pinned release is downloaded.

set -euo pipefail

REPO="${1:?usage: $0 <repo> <amd64-digest> <arm64-digest> <tag>...}"
AMD64="${2:?amd64 digest}"
ARM64="${3:?arm64 digest}"
shift 3
[[ $# -gt 0 ]] || { echo "error: no tags given" >&2; exit 1; }

HERE="$(cd "$(dirname "$0")" && pwd)"

for d in "$AMD64" "$ARM64"; do
    [[ "$d" =~ ^sha256:[0-9a-f]{64}$ ]] || { echo "error: '$d' is not a sha256 digest" >&2; exit 1; }
done

# Same pin as aikido's build-deps-image.sh (go-containerregistry checksums.txt).
CRANE_VERSION="v0.22.1"
CRANE_SHA256="0ab7a1d6932a213aed964ce97666c3077fe691c8606413674a8b3e0b9ec4cda0"
CRANE="${CRANE:-$(command -v crane || true)}"
if [[ -z "$CRANE" ]]; then
    tmp="$(mktemp -d)"
    curl -fsSL -o "$tmp/crane.tgz" \
        "https://github.com/google/go-containerregistry/releases/download/$CRANE_VERSION/go-containerregistry_Linux_x86_64.tar.gz"
    echo "$CRANE_SHA256  $tmp/crane.tgz" | sha256sum -c - >/dev/null \
        || { echo "error: crane sha256 mismatch" >&2; exit 1; }
    tar -xzf "$tmp/crane.tgz" -C "$tmp" crane
    CRANE="$tmp/crane"
fi

# Each input must be a single-platform image of the architecture it claims:
# a kaniko run that ignored --custom-platform would otherwise give an index
# with two amd64 entries.
for pair in "amd64:$AMD64" "arm64:$ARM64"; do
    arch="${pair%%:*}"; digest="${pair#*:}"
    got="$("$CRANE" config "$REPO@$digest" | python3 -I -c 'import json,sys; c=json.load(sys.stdin); print(c["os"]+"/"+c["architecture"])')"
    [[ "$got" == "linux/$arch" ]] \
        || { echo "error: $REPO@$digest is $got, expected linux/$arch" >&2; exit 1; }
done

first="$1"
echo "==> $REPO:$first <- linux/amd64 $AMD64 + linux/arm64 $ARM64"
# `index append` with no base starts an empty index and reads each platform
# from the image's config.
"$CRANE" index append -m "$REPO@$AMD64" -m "$REPO@$ARM64" -t "$REPO:$first"
INDEX="$("$CRANE" digest "$REPO:$first")"
for tag in "${@:2}"; do
    "$CRANE" tag "$REPO@$INDEX" "$tag"
done

status=0
for tag in "$@"; do
    "$CRANE" manifest "$REPO:$tag" \
        | python3 -I "$HERE/ci-image-platforms.py" --ref "$REPO:$tag" --digest "$INDEX" \
        || status=1
done
[[ $status == 0 ]] || exit 1

echo "$INDEX" > image-digest.txt
echo "index $INDEX -> image-digest.txt"
